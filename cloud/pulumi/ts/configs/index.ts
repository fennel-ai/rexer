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


interface map {
    [key: string]: string;
}

export type inputType = {
    kubeconfig: string,
    tierConfig: map,
    redisConfig: pulumi.Output<map>,
    cacheConfig: pulumi.Output<map>,
    dbConfig: pulumi.Output<map>,
    kafkaConfig: pulumi.Output<map>,
}

export type outputType = {}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        tierConfig: config.requireObject(nameof<inputType>("tierConfig")),
        kubeconfig: config.require(nameof<inputType>("kubeconfig")),
        redisConfig: config.requireSecretObject(nameof<inputType>("redisConfig")),
        cacheConfig: config.requireSecretObject(nameof<inputType>("cacheConfig")),
        dbConfig: config.requireSecretObject(nameof<inputType>("dbConfig")),
        kafkaConfig: config.requireSecretObject(nameof<inputType>("kafkaConfig")),
    }
}

export const setup = async (input: inputType) => {
    const provider = new k8s.Provider("k8s-provider", {
        kubeconfig: input.kubeconfig,
    })

    const rc = new k8s.core.v1.Secret("redis-config", {
        stringData: input.redisConfig,
        metadata: {
            namespace: "fennel",
            name: "redis-conf",
        },
    }, { provider, deleteBeforeReplace: true })

    const cacheConf = new k8s.core.v1.Secret("cache-config", {
        stringData: input.cacheConfig,
        metadata: {
            namespace: "fennel",
            name: "cache-conf"
        }
    }, { provider, deleteBeforeReplace: true })

    const kafkaCreds = new k8s.core.v1.Secret("kafka-config", {
        stringData: input.kafkaConfig,
        metadata: {
            name: "kafka-conf",
            namespace: "fennel",
        }
    }, { provider, deleteBeforeReplace: true })

    const dbCreds = new k8s.core.v1.Secret("db-config", {
        stringData: input.dbConfig,
        metadata: {
            namespace: "fennel",
            name: "mysql-conf",
        }
    }, { provider, deleteBeforeReplace: true })

    const tierConf = new k8s.core.v1.ConfigMap("tier-conf", {
        data: input.tierConfig,
        metadata: {
            namespace: "fennel",
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