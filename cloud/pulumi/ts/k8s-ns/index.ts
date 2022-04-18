import * as k8s from "@pulumi/kubernetes";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "kubernetes": "v3.18.0"
}

export type inputType = {
    kubeconfig: string,
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
