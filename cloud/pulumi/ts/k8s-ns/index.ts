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
}

export type outputType = {}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        kubeconfig: config.require(nameof<inputType>("kubeconfig")),
        namespace: config.require(nameof<inputType>("namespace")),
    }
}

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
