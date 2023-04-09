import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as k8s from "@pulumi/kubernetes";
import { local } from "@pulumi/command";

import { getPrefix, fennelStdTags, Scope, IngressConf, PRIVATE_LB_SCHEME, PUBLIC_LB_SCHEME } from "../lib/util";

const DEFAULT_INGRESS_NODE_TYPE = "t3.small";
const DEFAULT_INGRESS_NODE_COUNT = 2;
const DEFAULT_USE_DEDICATED_MACHINES = false;

export const plugins = {
    "kubernetes": "v3.20.1",
    "command": "v0.0.3",
    "aws": "v5.1.0",
}

export type inputType = {
    roleArn: string,
    region: string,
    clusterName: string | pulumi.Output<string>,
    nodeRoleArn: string | pulumi.Output<string>,
    kubeconfig: pulumi.Input<string>,
    namespace: string,
    privateSubnetIds: string[],
    publicSubnetIds: string[],
    ingressConf?: IngressConf,
    scopeId: number,
    scope: Scope,
    // TODO(Amit): This way of setting source IP ranges has a limitation that
    // we would need to update the stack everytime Cloudflare proxies IP ranges
    // are changed. This is not something very frequent though and we can live
    // with this for now.
    loadBalancerSourceIpRanges?: string[],
}

export type outputType = {
    loadBalancerUrl: string,
    endpontServiceName: string,
    tlsCert: string,
    tlsKey: string,
    tlsK8sSecretRef: string,
}

function getLoadBalancerName(url: string) {
    // ELBs have an associated hostname, that looks like this:
    // ${balancer_name}-${opaque_identifier}.${region}.elb.amazonaws.com
    const firstPart = url.split(".")[0];
    return firstPart.substring(0, firstPart.lastIndexOf("-"))
}

export const setup = async (input: inputType) => {
    const k8sProvider = new k8s.Provider("ingress-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
    })

    const awsProvider = new aws.Provider("ingress-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    // Setup http and https listeners as per instructions at:
    // https://www.getambassador.io/docs/edge-stack/latest/howtos/configure-communications/#basic-http-and-https
    const httplistener = new k8s.apiextensions.CustomResource("http-listener", {
        "apiVersion": "getambassador.io/v3alpha1",
        "kind": "Listener",
        "metadata": {
            "name": "http-listener"
        },
        "spec": {
            "port": 8080,
            "protocol": "HTTPS",
            "securityModel": "XFP",
            "hostBinding": {
                "namespace": {
                    "from": "SELF",
                }
            }
        }
    }, { provider: k8sProvider })

    const httpslistener = new k8s.apiextensions.CustomResource("https-listener", {
        "apiVersion": "getambassador.io/v3alpha1",
        "kind": "Listener",
        "metadata": {
            "name": "https-listener"
        },
        "spec": {
            "port": 8443,
            "protocol": "HTTPS",
            "securityModel": "XFP",
            "hostBinding": {
                "namespace": {
                    "from": "SELF",
                }
            }
        },
    }, { provider: k8sProvider })

    // by default use private subnets
    let subnetIds: string[];
    let loadBalancerScheme: string;
    if (input.ingressConf?.usePublicSubnets) {
        subnetIds = input.publicSubnetIds
        loadBalancerScheme = PUBLIC_LB_SCHEME
    } else {
        subnetIds = input.privateSubnetIds
        loadBalancerScheme = PRIVATE_LB_SCHEME
    }

    // Create dedicated node-group for emissary ingress pods
    //
    // This is to isolate them from getting scheduled on pods where the API servers are which could potentially
    // put pressure on node CPU or memory, affecting the availability of the emissary ingress pods (edge proxy for
    // the eks cluster).
    let topologySpreadConstraints: Record<string, any>[] = [];
    let nodeSelector: Record<string, string> = {
        "kubernetes.io/arch": "amd64",
        // we should schedule all components of Emissary on ON_DEMAND instances
        "eks.amazonaws.com/capacityType": "ON_DEMAND",
    };
    const replicas = input.ingressConf?.replicas || DEFAULT_INGRESS_NODE_COUNT;
    if (input.ingressConf?.useDedicatedMachines || DEFAULT_USE_DEDICATED_MACHINES) {
        const ngName = `aes-${input.namespace}-ng`;
        const ngLabel = { 'node-group': ngName };
        const nodeGroup = new aws.eks.NodeGroup(ngName, {
            clusterName: input.clusterName,
            nodeRoleArn: input.nodeRoleArn,
            subnetIds: subnetIds,
            scalingConfig: {
                desiredSize: replicas,
                minSize: replicas,
                // have maxSize to be at least one more than least or desired node count
                // so that emissary ingress's rolling update goes through (which creates a new instance
                // before replacing the original one).
                maxSize: replicas + 1,
            },
            instanceTypes: [DEFAULT_INGRESS_NODE_TYPE],
            nodeGroupNamePrefix: ngName,
            labels: ngLabel,
        }, { provider: awsProvider });

        // scheduled the pods on the dedicated node group created
        nodeSelector['node-group'] = ngName;
    }

    // Install emissary-ingress via helm.
    // NOTE: the name of the pulumi resource for the helm chart is also prefixed
    // to resource names. So if we're changing the name of the chart, we should also
    // change the lookup names of the emissary service/deployment in the transformation
    // spec and when looking up the URL.
    // We add a namespace to the name of the helm chart to avoid name collisions
    // with other ingresses in the same data plane.
    const chartName = `${input.namespace}-aes`;
    const version = "8.0.0";
    const emissaryIngress = new k8s.helm.v3.Release("emissary-ingress", {
        name: chartName,
        atomic: true,
        cleanupOnFail: true,
        repositoryOpts: {
            repo: "https://app.getambassador.io"
        },
        // helm Chart resource creation does not respect namespace field in the
        // provided k8s provider, so we explicitly specify the namespace here.
        namespace: input.namespace,
        chart: "emissary-ingress",
        version: version,
        forceUpdate: true,
        // Emissary ingress supports working across namespaces. Since we create similar listeners for different
        // tiers on the same cluster, emissary ingress routes requests across them. We scope the ingress to
        // respect a single namespace (namespace it is created in i.e. tier id).
        values: {
            "service": {
                "loadBalancerSourceRanges": input.loadBalancerSourceIpRanges != undefined ? input.loadBalancerSourceIpRanges : ["0.0.0.0/0"],
                "annotations": {
                    // Set load-balancer type as external to bypass k8s in-tree
                    // load-balancer controller and use AWS Load Balancer Controller
                    // instead.
                    // https://kubernetes-sigs.github.io/aws-load-balancer-controller/v2.3/guide/service/nlb/#configuration
                    "service.beta.kubernetes.io/aws-load-balancer-type": "external",
                    // Use NLB in instance mode since we don't currently setup the VPC CNI plugin.
                    "service.beta.kubernetes.io/aws-load-balancer-nlb-target-type": "instance",
                    // Specify the load balancer scheme. Should be one of ["internal", "internet-facing"].
                    "service.beta.kubernetes.io/aws-load-balancer-scheme": loadBalancerScheme,
                    // Specify the subnets in which to deploy the load balancer.
                    // For internet-facing load-balancers this should be a list of public subnets and
                    // for internal load-balancers this should be a list of private subnets.
                    // metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-subnets": input.subnetIds
                    "service.beta.kubernetes.io/aws-load-balancer-subnets": subnetIds.toString(),
                },
            },
            "deploymentAnnotations": {
                // We use inject=enabled instead of inject=ingress as per
                // https://github.com/linkerd/linkerd2/issues/6650#issuecomment-898732177.
                // Otherwise, we see the issue reported in the above bug report.
                "linkerd.io/inject": "enabled",
                "config.linkerd.io/skip-inbound-ports": "80,443",
            },
            "replicaCount": `${replicas}`,
            "scope": {
                "singleNamespace": true,
            },
            "namespace": {
                "name": input.namespace,
            },
            "ingressClassResource": {
                // Disable installing the ingress class resource provided by Emissary Ingress. For our use case,
                // using CRDs provided by Emissary Ingress is sufficient
                //
                // See - https://www.getambassador.io/docs/emissary/latest/topics/running/ingress-controller/#when-to-use-an-ingress-instead-of-annotations-or-crds
                "enabled": false,
            },
            "nodeSelector": nodeSelector,
            // Create a pod affinity to not place multiple emissary ingress pods on the same node.
            //
            // Same node restriction is enforced by the `topologyKey` field. Pod anti affinity behavior is determined by the
            // name of the pod which is `emissary-ingress`, hardcoded by the helm chart.
            "affinity": {
                "podAntiAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": [{
                        "labelSelector": {
                            "matchExpressions": [{
                                "key": "app.kubernetes.io/name",
                                "operator": "In",
                                "values": ["emissary-ingress"]
                            }]
                        },
                        "topologyKey": "kubernetes.io/hostname"
                    }
                    ],
                },
            },
            "agent": {
                "enabled": false,
            },
            // annotate emissary ingress pods such that the otel collector or self-hosted prometheus instance running
            // in the cluster is able to scrape the metrics reported by emissary ingress
            //
            // https://www.getambassador.io/docs/emissary/latest/howtos/prometheus/#productname
            "podAnnotations": {
                "prometheus.io/scrape": "true",
                // the port is the default value for the port of the admin service
                "prometheus.io/port": "8877",
            },
            "module": {
                "add_linkerd_headers": true,
                // https://www.getambassador.io/docs/edge-stack/latest/topics/using/circuit-breakers/ and
                //
                // https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/circuit_breaking.html
                //
                // NOTE: circuit breaker configured below is a GLOBAL circuit breaker. If there are more than one endpoint
                // configured in the future, consider making these limits at a `MAPPING` level.
                "circuit_breakers": [
                    {
                        // Specifies the maximum number of connections that Ambassador Edge Stack will make to ALL hosts in the upstream cluster.
                        "max_connections": 3072,
                        // Specifies the maximum number of requests that will be queued while waiting for a connection.
                        "max_pending_requests": 1024,
                        // Specifies the maximum number of parallel outstanding requests to ALL hosts in a cluster at any given time.
                        "max_requests": 3072,
                    }
                ],
                // See: https://github.com/emissary-ingress/emissary/issues/4329
                "envoy_log_type": "text",
                "envoy_log_format": "%REQ(:METHOD)% %RESPONSE_CODE% %RESPONSE_FLAGS% %RESPONSE_CODE_DETAILS% %CONNECTION_TERMINATION_DETAILS% %DURATION%",
                // Gzip enables Emissary-ingress to compress upstream data upon client request.
                "gzip": {
                    "compression_level": "DEFAULT_COMPRESSION",
                },
            }
        },
    }, {
        provider: k8sProvider,
        // Note: We ensure that listeners are created before the ingress helm
        // chart is deployed. See PR-505 for more details.
        dependsOn: [httplistener, httpslistener],
    });

    const loadBalancerUrl = k8s.core.v1.Service.get("ingress-svc", `${input.namespace}/${chartName}-emissary-ingress`, {
        provider: k8sProvider,
        dependsOn: emissaryIngress,
    }).status.loadBalancer.ingress[0].hostname;

    // Create TLS certificate for the generated url.
    // Setup root and issuer CA as per https://linkerd.io/2.11/tasks/generate-certificates/.
    const cmd = pulumi.interpolate`step certificate create fennel cert.pem key.pem --profile=self-signed --subtle --san=${loadBalancerUrl} --no-password --insecure -kty=RSA --size 4096`;
    const createCertificate = new local.Command(`${getPrefix(input.scope, input.scopeId)}-root-ca`, {
        create: cmd,
        delete: "rm -f cert.pem key.pem"
    })

    const cert = new local.Command("cert", {
        create: "cat cert.pem | base64"
    }, { dependsOn: createCertificate }).stdout

    const key = new local.Command("key", {
        create: "cat key.pem | base64"
    }, { dependsOn: createCertificate }).stdout

    const tlsK8sSecretRef = "tls-cert"
    const secret = new k8s.core.v1.Secret("tls", {
        type: "kubernetes.io/tls",
        metadata: {
            name: tlsK8sSecretRef,
        },
        data: {
            "tls.crt": cert,
            "tls.key": key,
        }
    }, { provider: k8sProvider })

    // Delete files
    new local.Command(`${getPrefix(input.scope, input.scopeId)}-cleanup`, {
        create: "rm -f cert.pem key.pem"
    }, { dependsOn: secret })

    const lb = loadBalancerUrl.apply(url => {
        return aws.lb.getLoadBalancer({
            name: getLoadBalancerName(url)
        }, { provider: awsProvider })
    })

    // TODO: VPC Endpoint service creation succeeds only when the NLB is in "Active" state.
    // Since LB is created in the background and might take time to become "Active", just adding an
    // dependency on `emissaryIngress` is not sufficient.
    // Figure out how creation of VPC Endpoint Service could wait on NLB coming to Active state. Use AWS SDK explicitly?
    const endpointServiceNamePrefix = getPrefix(input.scope, input.scopeId);
    const vpcEndpointService = new aws.ec2.VpcEndpointService(`${endpointServiceNamePrefix}-ingress-vpc-endpoint-service`, {
        acceptanceRequired: true,
        allowedPrincipals: [
            // Allow anyone to discover the service.
            "*",
        ],
        networkLoadBalancerArns: [lb.arn],
        tags: {
            ...fennelStdTags,
            "Name": `${input.namespace}-endpoint-service`
        },
    }, { provider: awsProvider, dependsOn: emissaryIngress })

    const output: pulumi.Output<outputType> = pulumi.output({
        loadBalancerUrl,
        tlsCert: cert,
        tlsKey: pulumi.secret(key),
        endpontServiceName: vpcEndpointService.serviceName,
        tlsK8sSecretRef: tlsK8sSecretRef,
    })

    return output
}
