export const serviceEnvs = [
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
    }
];

export const UNLEASH_USERNAME = "unleash";
// needs to be at least 8 characters
export const UNLEASH_PASSWORD = "unleash1";