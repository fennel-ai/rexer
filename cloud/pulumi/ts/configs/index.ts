import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
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
    "kubernetes": "v3.15.0"
}


export type inputType = {
    kubeconfig: string,
    namespace: string,
    tierConfig: Record<string, string>,
    redisConfig: pulumi.Output<Record<string, string>>,
    cacheConfig: pulumi.Output<Record<string, string>>,
    dbConfig: pulumi.Output<Record<string, string>>,
    kafkaConfig: pulumi.Output<Record<string, string>>,
}

export type outputType = {}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        namespace: config.require(nameof<inputType>("namespace")),
        kubeconfig: config.require(nameof<inputType>("kubeconfig")),
        tierConfig: config.requireObject(nameof<inputType>("tierConfig")),
        redisConfig: config.requireSecretObject(nameof<inputType>("redisConfig")),
        cacheConfig: config.requireSecretObject(nameof<inputType>("cacheConfig")),
        dbConfig: config.requireSecretObject(nameof<inputType>("dbConfig")),
        kafkaConfig: config.requireSecretObject(nameof<inputType>("kafkaConfig")),
    }
}

export const setup = async (input: inputType) => {
    const provider = new k8s.Provider("configs-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
    })

    const rc = new k8s.core.v1.Secret("redis-config", {
        stringData: input.redisConfig,
        metadata: {
            name: "redis-conf",
        },
    }, { provider, deleteBeforeReplace: true })

    const cacheConf = new k8s.core.v1.Secret("cache-config", {
        stringData: input.cacheConfig,
        metadata: {
            name: "cache-conf"
        }
    }, { provider, deleteBeforeReplace: true })

    const kafkaCreds = new k8s.core.v1.Secret("kafka-config", {
        stringData: input.kafkaConfig,
        metadata: {
            name: "kafka-conf",
        }
    }, { provider, deleteBeforeReplace: true })

    const dbCreds = new k8s.core.v1.Secret("db-config", {
        stringData: input.dbConfig,
        metadata: {
            name: "mysql-conf",
        }
    }, { provider, deleteBeforeReplace: true })

    const tierConf = new k8s.core.v1.ConfigMap("tier-conf", {
        data: input.tierConfig,
        metadata: {
            name: "tier-conf",
        }
    }, { provider, deleteBeforeReplace: true })
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
