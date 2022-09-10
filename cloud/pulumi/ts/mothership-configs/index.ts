import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";

export const plugins = {
    "kubernetes": "v3.20.1"
}


export type inputType = {
    kubeconfig: string | pulumi.Output<string>,
    namespace: string,
    mothershipConfig: Record<string, string>,
    dbConfig: pulumi.Input<Record<string, string>>,
}

export type outputType = {}

export const setup = async (input: inputType) => {
    const provider = new k8s.Provider("configs-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
    })

    const dbCreds = new k8s.core.v1.Secret("db-config", {
        stringData: input.dbConfig,
        metadata: {
            name: "mysql-conf",
        }
    }, { provider, deleteBeforeReplace: true })

    const mothershipConf = new k8s.core.v1.ConfigMap("mothership-conf", {
        data: input.mothershipConfig,
        metadata: {
            name: "mothership-conf",
        }
    }, { provider, deleteBeforeReplace: true });
    const output: outputType = {}
    return output
}
