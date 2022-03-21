import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as k8s from "@pulumi/kubernetes";
import { local } from "@pulumi/command";

import * as process from "process";
import { VpcEndpointService } from "@pulumi/aws/ec2";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "kubernetes": "v3.16.0",
    "command": "v0.0.3",
    "aws": "v4.38.1",
}

export type inputType = {
    roleArn: string,
    region: string,
    kubeconfig: pulumi.Input<string>,
    namespace: string,
    loadBalancerScheme: string,
    subnetIds: pulumi.Input<string[]>,
    tierId: number,
}

export type outputType = {
    loadBalancerUrl: string,
    endpontServiceName: string,
    tlsCert: string,
    tlsKey: string,
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        roleArn: config.require(nameof<inputType>("roleArn")),
        region: config.require(nameof<inputType>("region")),
        kubeconfig: config.require(nameof<inputType>("kubeconfig")),
        namespace: config.require(nameof<inputType>("namespace")),
        loadBalancerScheme: config.get(nameof<inputType>("loadBalancerScheme")) || "internal",
        subnetIds: config.requireObject(nameof<inputType>("subnetIds")),
        tierId: config.requireNumber(nameof<inputType>("tierId")),
    }
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

    // Install emissary-ingress via helm.
    // NOTE: the name of the pulumi resource for the helm chart is also prefixed
    // to resource names. So if we're changing the name of the chart, we should also
    // change the lookup names of the emissary service/deployment in the transformation
    // spec and when looking up the URL.
    // We add a namspace to the name of the helm chart to avoid name collisions
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
        version: "7.3.1",
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
                    metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-scheme"] = input.loadBalancerScheme
                    // Specify the subnets in which to deploy the load balancer.
                    // For internet-facing load-balancers this should be a list of public subnets and
                    // for internal load-balancers this should be a list of private subnets.
                    // metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-subnets"] = input.subnetIds
                    metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-subnets"] = input.subnetIds.toString()
                    obj.metadata = metadata
                    obj.spec["loadBalancerSourceRanges"] = ["0.0.0.0/0"]
                }
            },
        ],
        // Emissary ingress supports working across namespaces. Since we create similar listeners for different
        // tiers on the same cluster, emissary ingress routes requests across them. We scope the ingress to
        // respect a single namespace (namespace it is created in i.e. tier id).
        values: {
            "scope": {
                "singleNamespace": true,
            },
            "namespace": {
                "name": input.namespace,
            },
        },
    }, { provider: k8sProvider })

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
    }, { provider: k8sProvider, dependsOn: emissaryIngress.ready })

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
    }, { provider: k8sProvider, dependsOn: emissaryIngress.ready })

    const loadBalancerUrl = pulumi.all([input.namespace, emissaryIngress.ready]).apply(([namespace]) => {
        const ingressResource = emissaryIngress.getResource("v1/Service", namespace, `${chartName}-emissary-ingress`);
        return ingressResource.status.loadBalancer.ingress[0].hostname
    })

    // Create TLS certificate for the generated url.
    // Setup root and issuer CA as per https://linkerd.io/2.11/tasks/generate-certificates/.
    const cmd = loadBalancerUrl.apply(url => `step certificate create fennel cert.pem key.pem --profile=self-signed --subtle --san=${url} --no-password --insecure -kty=RSA --size 4096`)
    const createCertificate = new local.Command("root-ca", {
        create: cmd,
        delete: "rm -f cert.pem key.pem"
    })

    const cert = new local.Command("cert", {
        create: "cat cert.pem | base64"
    }, { dependsOn: createCertificate }).stdout

    const key = new local.Command("key", {
        create: "cat key.pem | base64"
    }, { dependsOn: createCertificate }).stdout

    const secret = new k8s.core.v1.Secret("tls", {
        type: "kubernetes.io/tls",
        metadata: {
            name: "tls-cert",
        },
        data: {
            "tls.crt": cert,
            "tls.key": key,
        }
    }, { provider: k8sProvider })

    // Delete files
    new local.Command("cleanup", {
        create: "rm -f cert.pem key.pem"
    }, { dependsOn: secret })

    const lb = loadBalancerUrl.apply(url => {
        return aws.lb.getLoadBalancer({
            name: getLoadBalancerName(url)
        }, { provider: awsProvider })
    })

    const vpcEndpointService = new aws.ec2.VpcEndpointService(`t-${input.tierId}-ingress-vpc-endpoint-service`, {
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
    }, { provider: awsProvider })

    const output: pulumi.Output<outputType> = pulumi.output({
        loadBalancerUrl,
        tlsCert: cert,
        tlsKey: pulumi.secret(key),
        endpontServiceName: vpcEndpointService.serviceName,
    })

    return output
}

async function run() {
    let output: pulumi.Output<outputType> | undefined;
    // Run the main function only if this program is run through the pulumi CLI.
    // Unfortunately, in that case the argv0 itself is not "pulumi", but the full
    // path of node: e.g. /nix/store/7q04aq0sq6im9a0k09gzfa1xfncc0xgm-nodejs-14.18.1/bin/node
    if (process.argv0 !== 'node') {
        pulumi.log.info("Running...")
        const input: inputType = parseConfig();
        output = await setup(input)
    }
    return output
}


export const output = await run();
