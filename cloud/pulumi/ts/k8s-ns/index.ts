import * as pulumi from "@pulumi/pulumi"
import * as k8s from "@pulumi/kubernetes";
import { PulumiFn } from "@pulumi/pulumi/automation";

export const plugins = {
    "kubernetes": "v3.20.1"
}

export type inputType = {
    kubeconfig: string | pulumi.Output<string>,
    namespace: string,
}

export type outputType = {}

export const setup = async (input: inputType) => {
    const k8sProvider = new k8s.Provider("ns-k8s-provider", {
        kubeconfig: input.kubeconfig,
    })

    const ns = new k8s.core.v1.Namespace(`namespace-${input.namespace}`, {
        metadata: {
            name: input.namespace,
            annotations: {
                "linkerd.io/inject": "enabled",
            },
        }
    }, { provider: k8sProvider })

    const output: outputType = {}
    return output
}
