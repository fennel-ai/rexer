import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";

export const plugins = {
    "kubernetes": "v3.18.0"
}


export type inputType = {
    kubeconfig: string,
    namespace: string,
    tierConfig: Record<string, string>,
    redisConfig: pulumi.Output<Record<string, string>>,
    cacheConfig: pulumi.Output<Record<string, string>>,
    dbConfig: pulumi.Output<Record<string, string>>,
    kafkaConfig: pulumi.Output<Record<string, string>>,
    modelServingConfig: pulumi.Output<Record<string, string>>,
    glueConfig: pulumi.Output<Record<string, string>>,
    unleashConfig: pulumi.Output<Record<string, string>>,
    otelCollectorConfig: pulumi.Output<Record<string, string>>,
    offlineAggregateOutputConfig: pulumi.Output<Record<string, string>>,
    milvusConfig: pulumi.Output<Record<string, string>>,
}

export type outputType = {}

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

    const sagemakerConf = new k8s.core.v1.Secret("model-serving-config", {
        stringData: input.modelServingConfig,
        metadata: {
            name: "model-serving-conf",
        }
    }, { provider, deleteBeforeReplace: true });

    const glueConf = new k8s.core.v1.Secret("glue-config", {
        stringData: input.glueConfig,
        metadata: {
            name: "glue-conf",
        }
    }, { provider, deleteBeforeReplace: true });

    const tierConf = new k8s.core.v1.ConfigMap("tier-conf", {
        data: input.tierConfig,
        metadata: {
            name: "tier-conf",
        }
    }, { provider, deleteBeforeReplace: true });

    const unleashConf = new k8s.core.v1.ConfigMap("unleash-conf", {
        data: input.unleashConfig,
        metadata: {
            name: "unleash-conf",
        }
    }, { provider, deleteBeforeReplace: true });

    const otelCollectorConf = new k8s.core.v1.ConfigMap("otel-collector-conf", {
        data: input.otelCollectorConfig,
        metadata: {
            name: "otel-collector-conf",
        }
    }, { provider, deleteBeforeReplace: true });

    const offlineAggregateOutputConf = new k8s.core.v1.ConfigMap("offline-aggr-output-conf", {
        data: input.offlineAggregateOutputConfig,
        metadata: {
            name: "offline-aggregate-output-conf",
        }
    }, { provider, deleteBeforeReplace: true });

    const milvusConf = new k8s.core.v1.ConfigMap("milvus-conf", {
        data: input.milvusConfig,
        metadata: {
            name: "milvus-conf",
        }
    }, { provider, deleteBeforeReplace: true });

    const output: outputType = {}
    return output
}
