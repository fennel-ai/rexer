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
        }
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

        // create affinity such that they are not scheduled in the same zone, however do not make this as a "strict"
        // restriction i.e. allow it to be scheduled in case both the nodes are in the same AZs as well (which should
        // not ideally happen with Managed Node Groups).
        topologySpreadConstraints = [{
            // do not allow > 1 skew i.e. diff of pods on domain topologies should never be above 1
            "maxSkew": 1,
            // use zone as the domain topology i.e. pods in the same zone count towards computing maxSkew
            "topologyKey": "topology.kubernetes.io/zone",
            "whenUnsatisfiable": "ScheduleAnyway",
        }];
    }


    // Install emissary-ingress via helm.
    // NOTE: the name of the pulumi resource for the helm chart is also prefixed
    // to resource names. So if we're changing the name of the chart, we should also
    // change the lookup names of the emissary service/deployment in the transformation
    // spec and when looking up the URL.
    // We add a namespace to the name of the helm chart to avoid name collisions
    // with other ingresses in the same data plane.
    const chartName = `aes-${input.namespace}`;
    const emissaryIngress = new k8s.helm.v3.Chart(chartName, {
        fetchOpts: {
            repo: "https://app.getambassador.io"
        },
        // helm Chart resource creation does not respect namespace field in the
        // provided k8s provider, so we explicitly specify the namespace here.
        namespace: input.namespace,
        chart: "emissary-ingress",
        version: "8.0.0",
        transformations: [
            (obj: any, opts: pulumi.CustomResourceOptions) => {
                if (obj.kind === "Deployment" && obj.metadata.name === `${chartName}-emissary-ingress`) {
                    const metadata = obj.spec.template.metadata || {}
                    metadata.annotations = metadata.annotations || {}
                    // We use inject=enabled instead of inject=ingress as per
                    // https://github.com/linkerd/linkerd2/issues/6650#issuecomment-898732177.
                    // Otherwise, we see the issue reported in the above bug report.
                    metadata.annotations["linkerd.io/inject"] = "enabled"
                    metadata.annotations["config.linkerd.io/skip-inbound-ports"] = "80,443"
                    obj.spec.template.metadata = metadata
                }
            },
            (obj: any, opts: pulumi.CustomResourceOptions) => {
                if (obj.kind === "Service" && obj.spec.type === "LoadBalancer") {
                    const metadata = obj.metadata || {}
                    metadata.annotations = metadata.annotations || {}
                    // Set load-balancer type as external to bypass k8s in-tree
                    // load-balancer controller and use AWS Load Balancer Controller
                    // instead.
                    // https://kubernetes-sigs.github.io/aws-load-balancer-controller/v2.3/guide/service/nlb/#configuration
                    metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-type"] = "external"
                    // Use NLB in instance mode since we don't currently setup the VPC CNI plugin.
                    metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-nlb-target-type"] = "instance"
                    // Specify the load balancer scheme. Should be one of ["internal", "internet-facing"].
                    metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-scheme"] = loadBalancerScheme
                    // Specify the subnets in which to deploy the load balancer.
                    // For internet-facing load-balancers this should be a list of public subnets and
                    // for internal load-balancers this should be a list of private subnets.
                    // metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-subnets"] = input.subnetIds
                    metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-subnets"] = subnetIds.toString()
                    obj.metadata = metadata
                    obj.spec["loadBalancerSourceRanges"] = ["0.0.0.0/0"]
                }
            },
        ],
        // Emissary ingress supports working across namespaces. Since we create similar listeners for different
        // tiers on the same cluster, emissary ingress routes requests across them. We scope the ingress to
        // respect a single namespace (namespace it is created in i.e. tier id).
        values: {
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
            "topologySpreadConstraints": topologySpreadConstraints,
            "nodeSelector": nodeSelector,

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
            }
        },
    }, {
        provider: k8sProvider,
        // Note: We ensure that listeners are created before the ingress helm
        // chart is deployed. See PR-505 for more details.
        dependsOn: [httplistener, httpslistener],
    })


    const loadBalancerUrl = pulumi.all([input.namespace, emissaryIngress.ready]).apply(([namespace]) => {
        const ingressResource = emissaryIngress.getResource("v1/Service", namespace, `${chartName}-emissary-ingress`);
        return ingressResource.status.loadBalancer.ingress[0].hostname
    })

    // Create TLS certificate for the generated url.
    // Setup root and issuer CA as per https://linkerd.io/2.11/tasks/generate-certificates/.
    const cmd = loadBalancerUrl.apply(url => `step certificate create fennel cert.pem key.pem --profile=self-signed --subtle --san=${url} --no-password --insecure -kty=RSA --size 4096`)
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
    }, { provider: awsProvider, dependsOn: emissaryIngress.ready })

    const output: pulumi.Output<outputType> = pulumi.output({
        loadBalancerUrl,
        tlsCert: cert,
        tlsKey: pulumi.secret(key),
        endpontServiceName: vpcEndpointService.serviceName,
        tlsK8sSecretRef: tlsK8sSecretRef,
    })

    return output
}
