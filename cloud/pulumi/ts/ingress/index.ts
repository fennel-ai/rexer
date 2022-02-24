import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";

import * as process from "process";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "kubernetes": "v3.16.0"
}

export type inputType = {
    kubeconfig: string,
    namespace: string,
    loadBalancerScheme: string,
    subnetIds: string[],
}

export type outputType = {
    loadBalancerUrl: pulumi.Output<string>
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        kubeconfig: config.require(nameof<inputType>("kubeconfig")),
        namespace: config.require(nameof<inputType>("namespace")),
        loadBalancerScheme: config.get(nameof<inputType>("loadBalancerScheme")) || "internal",
        subnetIds: config.requireObject(nameof<inputType>("subnetIds")),
    }
}

export const setup = async (input: inputType) => {
    const k8sProvider = new k8s.Provider("k8s-provider", {
        kubeconfig: input.kubeconfig,
    })

    const ns = new k8s.core.v1.Namespace("ingress-ns", {
        metadata: {
            name: input.namespace,
        }
    }, { provider: k8sProvider })

    // Install emissary-ingress via helm.
    const emissaryIngress = new k8s.helm.v3.Chart("ingress", {
        fetchOpts: {
            repo: "https://app.getambassador.io"
        },
        chart: "emissary-ingress",
        version: "7.3.1",
        namespace: input.namespace,
        transformations: [
            (obj: any, opts: pulumi.CustomResourceOptions) => {
                if (obj.kind === "Deployment" && obj.metadata.name === "emissary-ingress") {
                    const metadata = obj.spec.template.metadata
                    metadata.annotations = metadata.annotations || {}
                    // We use inject=enabled instead of inject=ingress as per
                    // https://github.com/linkerd/linkerd2/issues/6650#issuecomment-898732177.
                    // Otherwise, we see the issue reported in the above bug report.
                    metadata.annotations["linkerd.io/inject"] = "enabled"
                    metadata.annotations["config.linkerd.io/skip-inbound-ports"] = "80,443"
                }
            },
            (obj: any, opts: pulumi.CustomResourceOptions) => {
                if (obj.kind === "Service" && obj.spec.type === "LoadBalancer") {
                    const metadata = obj.metadata
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
                    metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-subnets"] = input.subnetIds
                }
            },
        ]
    }, { provider: k8sProvider })

    const loadBalancerUrl = emissaryIngress.ready.apply((_) => {
        const ingressResource = emissaryIngress.getResource("v1/Service", input.namespace, "emissary-ingress");
        return ingressResource.status.loadBalancer.ingress[0].hostname
    })

    const output: outputType = {
        loadBalancerUrl,
    }

    return output
}

async function run() {
    let output: outputType | undefined;
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