import {INSTANCE_METADATA_SERVICE_ADDR} from "../lib/util";

export const serviceEnvs = [
    {
        name: "NITROUS_SERVER_ADDRESS",
        valueFrom: {
            configMapKeyRef: {
                name: "nitrous-conf",
                key: "addr",
            }
        }
    },
    {
        name: "KAFKA_SERVER_ADDRESS",
        valueFrom: {
            secretKeyRef: {
                name: "kafka-conf",
                key: "server",
            }
        }
    },
    {
        name: "KAFKA_USERNAME",
        valueFrom: {
            secretKeyRef: {
                name: "kafka-conf",
                key: "username",
            }
        }
    },
    {
        name: "KAFKA_PASSWORD",
        valueFrom: {
            secretKeyRef: {
                name: "kafka-conf",
                key: "password",
            }
        }
    },
    {
        name: "PRODUCE_TO_CONFLUENT",
        valueFrom: {
            secretKeyRef: {
                name: "kafka-conf",
                key: "topicProducesToConfluent",
            }
        }
    },
    {
        name: "MSK_KAFKA_SERVER_ADDRESS",
        valueFrom: {
            secretKeyRef: {
                name: "msk-kafka-conf",
                key: "mskServers",
            }
        }
    },
    {
        name: "MSK_KAFKA_USERNAME",
        valueFrom: {
            secretKeyRef: {
                name: "msk-kafka-conf",
                key: "mskUsername",
            }
        }
    },
    {
        name: "MSK_KAFKA_PASSWORD",
        valueFrom: {
            secretKeyRef: {
                name: "msk-kafka-conf",
                key: "mskPassword",
            }
        }
    },
    {
        name: "REDIS_SERVER_ADDRESS",
        valueFrom: {
            secretKeyRef: {
                name: "redis-conf",
                key: "addr",
            }
        }
    },
    {
        name: "MYSQL_SERVER_ADDRESS",
        valueFrom: {
            secretKeyRef: {
                name: "mysql-conf",
                key: "host",
            }
        }
    },
    {
        name: "MYSQL_DATABASE_NAME",
        valueFrom: {
            secretKeyRef: {
                name: "mysql-conf",
                key: "db",
            }
        }
    },
    {
        name: "MYSQL_USERNAME",
        valueFrom: {
            secretKeyRef: {
                name: "mysql-conf",
                key: "username",
            }
        }
    },
    {
        name: "MYSQL_PASSWORD",
        valueFrom: {
            secretKeyRef: {
                name: "mysql-conf",
                key: "password",
            }
        }
    },
    {
        name: "TIER_ID",
        valueFrom: {
            configMapKeyRef: {
                name: "tier-conf",
                key: "tier_id",
            }
        }
    },
    {
        name: "PLANE_ID",
        valueFrom: {
            configMapKeyRef: {
                name: "tier-conf",
                key: "plane_id",
            }
        }
    },
    {
        name: "CACHE_PRIMARY",
        valueFrom: {
            secretKeyRef: {
                name: "cache-conf",
                key: "primary",
            }
        }
    },
    {
        name: "AWS_REGION",
        valueFrom: {
            secretKeyRef: {
                name: "model-serving-conf",
                key: "region",
            }
        }
    },
    {
        name: "SAGEMAKER_EXECUTION_ROLE",
        valueFrom: {
            secretKeyRef: {
                name: "model-serving-conf",
                key: "executionRole",
            }
        }
    },
    {
        name: "PRIVATE_SUBNETS",
        valueFrom: {
            secretKeyRef: {
                name: "model-serving-conf",
                key: "privateSubnets",
            }
        }
    },
    {
        name: "SAGEMAKER_SECURITY_GROUP",
        valueFrom: {
            secretKeyRef: {
                name: "model-serving-conf",
                key: "securityGroup",
            }
        }
    },
    {
        name: "SAGEMAKER_INSTANCE_TYPE",
        valueFrom: {
            secretKeyRef: {
                name: "model-serving-conf",
                key: "instanceType",
            }
        }
    },
    {
        name: "SAGEMAKER_INSTANCE_COUNT",
        valueFrom: {
            secretKeyRef: {
                name: "model-serving-conf",
                key: "instanceCount",
            }
        }
    },
    {
        name: "MODEL_STORE_S3_BUCKET",
        valueFrom: {
            secretKeyRef: {
                name: "model-serving-conf",
                key: "modelStoreBucket",
            }
        }
    },
    {
        name: "MODEL_STORE_ENDPOINT",
        valueFrom: {
            secretKeyRef: {
                name: "model-serving-conf",
                key: "modelStoreEndpoint",
            }
        }
    },
    {
        name: "JOB_NAME_BY_AGG",
        valueFrom: {
            secretKeyRef: {
                name: "glue-conf",
                key: "jobNameByAgg",
            }
        }
    },
    {
        name: "UNLEASH_ENDPOINT",
        valueFrom: {
            configMapKeyRef: {
                name: "unleash-conf",
                key: "endpoint",
            }
        }
    },
    {
        name: "OTLP_ENDPOINT",
        valueFrom: {
            configMapKeyRef: {
                name: "otel-collector-conf",
                key: "endpoint"
            }
        }
    },
    {
        name: "OTLP_HTTP_ENDPOINT",
        valueFrom: {
            configMapKeyRef: {
                name: "otel-collector-conf",
                key: "httpEndpoint"
            }
        }
    },
    {
        name: "OFFLINE_AGG_BUCKET",
        valueFrom: {
            configMapKeyRef: {
                name: "offline-aggregate-output-conf",
                key: "bucket"
            }
        }
    },
    {
        name: "MILVUS_URL",
        valueFrom: {
            configMapKeyRef: {
                name: "milvus-conf",
                key: "endpoint"
            }
        }
    },
    {
        name: "PROCESS_ID",
        valueFrom: {
            fieldRef: {
                fieldPath: "metadata.name"
            }
        }
    },
    {
        name: "PPROF_BUCKET",
        valueFrom: {
            configMapKeyRef: {
                name: "pprof-conf",
                key: "bucket",
            }
        }
    },
    {
        name: "AIRBYTE_SERVER_ADDRESS",
        valueFrom: {
            configMapKeyRef: {
                name: "airbyte-conf",
                key: "endpoint"
            }
        }
    },
    {
        name: "INSTANCE_METADATA_SERVICE_ADDR",
        value: INSTANCE_METADATA_SERVICE_ADDR
    }
];

export const POSTGRESQL_USERNAME = "username";
// needs to be at least 8 characters
export const POSTGRESQL_PASSWORD = "password";

export function ReadinessProbe(metricPort: number) {
    return {
        initialDelaySeconds: 2,
        periodSeconds: 2,
        successThreshold: 1,
        timeoutSeconds: 1,
        httpGet: {
            path: "/ready",
            port: metricPort
        }
    }
}