import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";

export const plugins = {
    "kubernetes": "v3.20.1"
}


export type inputType = {
    kubeconfig: string,
    namespace: string,
    tierConfig: Record<string, string>,
    redisConfig: pulumi.Input<Record<string, string>>,
    cacheConfig: pulumi.Input<Record<string, string>>,
    dbConfig: pulumi.Input<Record<string, string>>,
    mskConfig: pulumi.Input<Record<string, string>>,
    modelServingConfig: pulumi.Input<Record<string, string>>,
    glueConfig: pulumi.Input<Record<string, string>>,
    unleashConfig: pulumi.Input<Record<string, string>>,
    otelCollectorConfig: pulumi.Input<Record<string, string>>,
    offlineAggregateOutputConfig: pulumi.Input<Record<string, string>>,
    milvusConfig: pulumi.Input<Record<string, string>>,
    pprofConfig: pulumi.Input<Record<string, string>>,
    nitrousConfig: pulumi.Input<Record<string, string>>,
    airbyteConfig: pulumi.Input<Record<string, string>>,
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

    const mskCreds = new k8s.core.v1.Secret("msk-kafka-config", {
        stringData: input.mskConfig,
        metadata: {
            name: "msk-kafka-conf",
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

    const pprofConf = new k8s.core.v1.ConfigMap("pprof-conf", {
        data: input.pprofConfig,
        metadata: {
            name: "pprof-conf"
        }
    }, { provider, deleteBeforeReplace: true });

    const nitrousConf = new k8s.core.v1.ConfigMap("nitrous-conf", {
        data: input.nitrousConfig,
        metadata: {
            name: "nitrous-conf"
        },
    }, { provider, deleteBeforeReplace: true });

    const airbyteConf = new k8s.core.v1.ConfigMap("airbyte-conf", {
        data: input.airbyteConfig,
        metadata: {
            name: "airbyte-conf"
        },
    }, { provider, deleteBeforeReplace: true });

    const output: outputType = {}
    return output
}
