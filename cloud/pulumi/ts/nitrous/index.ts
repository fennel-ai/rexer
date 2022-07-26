import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws"
import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as path from "path";
import * as kafka from "@pulumi/kafka";
import * as process from "process";
import * as childProcess from "child_process";
import * as util from "../lib/util";
import { NONAME } from "dns";


export const plugins = {
    "aws": "v5.1.0",
    "kubernetes": "v3.18.0",
    "docker": "v3.2.0",
    "kafka": "v3.3.0",
}

// Nitrous service configuration.
const DEFAULT_REPLICAS = 1
const DEFAULT_USE_AMD64 = false
// By default, we want nitrous replicas to be isolated from each other.
const DEFAULT_FORCE_REPLICA_ISOLATION = true

// Binlog configuration.
const DEFAULT_BINLOG_REPLICATION_FACTOR = 3;
const DEFAULT_BINLOG_PARTITIONS = 1;
const BINLOG_TOPIC_NAME = "nitrous_log"

// default for resource requirement configurations
const DEFAULT_CPU_REQUEST = "1000m"
const DEFAULT_CPU_LIMIT = "1500m"
const DEFAULT_MEMORY_REQUEST = "2G"
const DEFAULT_MEMORY_LIMIT = "4G"

export const name = "nitrous"
export const namespace = "fennel"
export const servicePort = 3333;
const root = process.env["FENNEL_ROOT"]!

export type binlogConfig = {
    partitions?: number,
    replicationFactor?: number,
    // Maximum time after which old messages (grouped as segments) are discarded to free up space
    //
    // Set -1 for "unlimited" retention
    retention_ms?: number,
    // Maximum size of a partition can grow to before old messages (grouped as segments) are discarded to free up space
    //
    // NOTE: This is configured per partition, therefore for a topic with multiple partitions, this should be
    // multiplied with the number of partitions to estimate the max occupied capacity
    //
    // Set -1 to configure no discards based on the size of the partition
    partition_retention_bytes?: number,
}

export type kafkaAdmin = {
    apiKey: pulumi.Input<string>,
    apiSecret: pulumi.Input<string>
    bootstrapServer: pulumi.Input<string>,
}

export type inputType = {
    planeId: number,
    region: string,
    roleArn: pulumi.Input<string>,
    kubeconfig: pulumi.Input<any>,
    otlpEndpoint: pulumi.Input<string>,

    replicas?: number,
    useAmd64?: boolean,
    enforceReplicaIsolation?: boolean,
    resourceConf?: util.ResourceConf
    nodeLabels?: Record<string, string>,

    storageClass?: pulumi.Input<string>,
    storageCapacityGB: number
    blockCacheMB: number
    kvCacheMB: number

    kafka: kafkaAdmin,
    binlog: binlogConfig,

    protect: boolean,
}

export type outputType = {
    appLabels: { [key: string]: string },
    svc: k8s.core.v1.Service,
}

function setupBinlog(input: inputType) {
    const kafkaProvider = new kafka.Provider("nitrous-kafka-provider", {
        bootstrapServers: [input.kafka.bootstrapServer],
        saslUsername: input.kafka.apiKey,
        saslPassword: input.kafka.apiSecret,
        saslMechanism: "plain",
        tlsEnabled: true,
    })
    const config = {
        "retention.ms": input.binlog.retention_ms,
        "retention.bytes": input.binlog.partition_retention_bytes,
    };
    const topic = new kafka.Topic(`topic-p-${input.planeId}-${BINLOG_TOPIC_NAME}`, {
        name: `p_${input.planeId}_${BINLOG_TOPIC_NAME}`,
        partitions: input.binlog.partitions || DEFAULT_BINLOG_PARTITIONS,
        // We set replication factor to 3 regardless of the cluster availability
        // since that's the minimum required by confluent cloud:
        // https://github.com/Mongey/terraform-provider-kafka/issues/40#issuecomment-456897983
        replicationFactor: input.binlog.replicationFactor || DEFAULT_BINLOG_REPLICATION_FACTOR,
        config: config,
    }, { provider: kafkaProvider, protect: input.protect })

    const k8sProvider = new k8s.Provider("configs-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: namespace,
    })
    const kafkaCreds = new k8s.core.v1.Secret("kafka-config", {
        stringData: {
            "server": input.kafka.bootstrapServer,
            "username": input.kafka.apiKey,
            "password": input.kafka.apiSecret,
        },
        metadata: {
            name: "kafka-conf",
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true })
}

function createBackupService(input: inputType, k8sProvider: k8s.Provider,
    imageName: pulumi.Input<string>, bucketName: string) {
    const name = "nitrous-backup"
    const appLabels = { app: name };
    const metricsPort = 2112;
    let platformConfig = getPlatformSpecificConfig(input)

    const nitrousBackupSvc = new k8s.core.v1.Service("nitrous-backup-svc", {
        metadata: {
            labels: appLabels,
            name: name,
        },
        spec: {
            clusterIP: "None",
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    const backupStatefulState = new k8s.apps.v1.StatefulSet("nitrous-backup-statefulset", {
        metadata: {
            name: "nitrous-backup",
            labels: appLabels,
        },
        spec: {
            serviceName: nitrousBackupSvc.metadata.name,
            selector: { matchLabels: appLabels },
            replicas: 1,
            template: {
                metadata: {
                    labels: appLabels,
                    annotations: {
                        // Skip Linkerd protocol detection for mysql and redis
                        // instances running outside the cluster.
                        // See: https://linkerd.io/2.11/features/protocol-detection/.
                        "config.linkerd.io/skip-outbound-ports": "3306,6379",
                        "prometheus.io/scrape": "true",
                        "prometheus.io/port": metricsPort.toString(),
                    }
                },
                spec: {
                    nodeSelector: platformConfig.nodeSelector,
                    containers: [
                        {
                            command: [
                                "/root/nitrous",
                                "--region",
                                `${input.region}`,
                                "--listen-port",
                                `${servicePort}`,
                                "--metrics-port",
                                `${metricsPort}`,
                                "--plane-id",
                                `${input.planeId}`,
                                "--badger_dir",
                                "/oxide",
                                "--badger_block_cache_mb",
                                `${input.blockCacheMB}`,
                                "--ristretto_max_cost",
                                (input.kvCacheMB << 20).toString(),
                                "--otlp-endpoint",
                                input.otlpEndpoint,
                                "--backup-bucket",
                                bucketName,
                                "--shard-name",
                                "default",
                                "--dev=false",
                                "--backup-node",
                            ],
                            name: name,
                            image: imageName,
                            imagePullPolicy: "Always",
                            ports: [
                                {
                                    containerPort: metricsPort,
                                    protocol: "TCP",
                                },
                            ],
                            env: [
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
                            ],
                            resources: {
                                requests: {
                                    "cpu": input.resourceConf?.cpu.request || DEFAULT_CPU_REQUEST,
                                    "memory": input.resourceConf?.memory.request || DEFAULT_MEMORY_REQUEST,
                                },
                                limits: {
                                    "cpu": input.resourceConf?.cpu.limit || DEFAULT_CPU_LIMIT,
                                    "memory": input.resourceConf?.memory.limit || DEFAULT_MEMORY_LIMIT,
                                }
                            },
                            volumeMounts: [
                                {
                                    name: "badgerdb",
                                    mountPath: "/oxide",
                                }
                            ],
                        },
                    ],
                },
            },
            volumeClaimTemplates: [
                {
                    metadata: {
                        name: "badgerdb",
                    },
                    spec: {
                        accessModes: ["ReadWriteOnce"],
                        // if the storage class is undefined, default storage class is used by the PVC.
                        storageClassName: input.storageClass,
                        resources: {
                            requests: {
                                storage: `${input.storageCapacityGB}Gi`,
                            },
                        }
                    }
                }
            ]
        },
    }, { provider: k8sProvider, deleteBeforeReplace: true })
}

function getPlatformSpecificConfig(input: inputType) {
    let nodeSelector = input.nodeLabels || {};
    let dockerfile, platform;
    if (input.useAmd64 || DEFAULT_USE_AMD64) {
        dockerfile = path.join(root, "dockerfiles/nitrous.dockerfile")
        platform = "linux/amd64"
        nodeSelector["kubernetes.io/arch"] = "amd64"
    } else {
        dockerfile = path.join(root, "dockerfiles/nitrous_arm64.dockerfile")
        platform = "linux/arm64"
        nodeSelector["kubernetes.io/arch"] = "arm64"
    }
    return {
        "dockerfile": dockerfile,
        "platform": platform,
        "nodeSelector": nodeSelector,
    }
}

export const setup = async (input: inputType) => {

    // Setup binlog kafka topic.
    const topic = setupBinlog(input)

    const awsProvider = new aws.Provider("nitrous-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const bucketName = `nitrous-p-${input.planeId}-backup`
    const bucket = new aws.s3.Bucket(bucketName, {
        acl: "private",
        bucket: bucketName,
        // delete all the objects so that the bucket can be deleted without error
        forceDestroy: true,
    }, { provider: awsProvider, protect: input.protect });


    // Create a private ECR repository.
    const repo = new aws.ecr.Repository(`p-${input.planeId}-nitrous-repo`, {
        imageScanningConfiguration: {
            scanOnPush: true
        },
        imageTagMutability: "MUTABLE"
    }, { provider: awsProvider });

    // Get registry info (creds and endpoint).
    const registryInfo = repo.registryId.apply(async id => {
        const credentials = await aws.ecr.getCredentials({ registryId: id }, { provider: awsProvider });
        const decodedCredentials = Buffer.from(credentials.authorizationToken, "base64").toString();
        const [username, password] = decodedCredentials.split(":");
        if (!password || !username) {
            throw new Error("Invalid credentials");
        }
        return {
            server: credentials.proxyEndpoint,
            username: username,
            password: password,
        };
    });

    // Get the (hash) commit id.
    // NOTE: This requires git to be installed and DOES NOT take local changes or commits into consideration.
    const hashId = childProcess.execSync('git rev-parse --short HEAD').toString().trim()
    const imageName = repo.repositoryUrl.apply(imgName => {
        return `${imgName}:${hashId}`
    })

    // Build and publish the container image.
    let platformConfig = getPlatformSpecificConfig(input)
    const image = new docker.Image("nitrous-img", {
        build: {
            context: root,
            dockerfile: platformConfig.dockerfile,
            args: {
                "platform": platformConfig.platform,
            },
        },
        imageName: imageName,
        registry: registryInfo,
    });

    const k8sProvider = new k8s.Provider("nitrous-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: namespace,
    })

    // Create a load balanced Kubernetes service using this image, and export its IP.
    const appLabels = { app: name };
    const metricsPort = 2112;

    const forceReplicaIsolation = input.enforceReplicaIsolation || DEFAULT_FORCE_REPLICA_ISOLATION;
    let whenUnsatisfiable = "ScheduleAnyway";
    if (forceReplicaIsolation) {
        whenUnsatisfiable = "DoNotSchedule";
    }

    if (input.storageClass === undefined) {
        console.log('storageClass is undefined for Nitrous - will use default storage class for persistent volume.')
    }

    const nitrousSvc = new k8s.core.v1.Service("nitrous-svc", {
        metadata: {
            labels: appLabels,
            name: name,
        },
        spec: {
            type: "ClusterIP",
            ports: [{ port: servicePort, targetPort: servicePort, protocol: "TCP" }],
            selector: appLabels,
        },
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    const nitrousStatefulSet = new k8s.apps.v1.StatefulSet("nitrous-statefulset", {
        metadata: {
            name: "nitrous",
            labels: appLabels,
        },
        spec: {
            serviceName: nitrousSvc.metadata.name,
            selector: { matchLabels: appLabels },
            replicas: input.replicas || DEFAULT_REPLICAS,
            template: {
                metadata: {
                    labels: appLabels,
                    annotations: {
                        // Skip Linkerd protocol detection for mysql and redis
                        // instances running outside the cluster.
                        // See: https://linkerd.io/2.11/features/protocol-detection/.
                        "config.linkerd.io/skip-outbound-ports": "3306,6379",
                        "prometheus.io/scrape": "true",
                        "prometheus.io/port": metricsPort.toString(),
                    }
                },
                spec: {
                    nodeSelector: platformConfig.nodeSelector,
                    containers: [
                        {
                            command: [
                                "/root/nitrous",
                                "--region",
                                `${input.region}`,
                                "--listen-port",
                                `${servicePort}`,
                                "--metrics-port",
                                `${metricsPort}`,
                                "--plane-id",
                                `${input.planeId}`,
                                "--badger_dir",
                                "/oxide",
                                "--badger_block_cache_mb",
                                `${input.blockCacheMB}`,
                                "--ristretto_max_cost",
                                (input.kvCacheMB << 20).toString(),
                                "--otlp-endpoint",
                                input.otlpEndpoint,
                                "--backup-bucket",
                                bucketName,
                                "--shard-name",
                                "default",
                                "--dev=false",
                            ],
                            name: name,
                            image: image.imageName,
                            imagePullPolicy: "Always",
                            ports: [
                                {
                                    containerPort: servicePort,
                                    protocol: "TCP",
                                },
                                {
                                    containerPort: metricsPort,
                                    protocol: "TCP",
                                },
                            ],
                            env: [
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
                            ],
                            resources: {
                                requests: {
                                    "cpu": input.resourceConf?.cpu.request || DEFAULT_CPU_REQUEST,
                                    "memory": input.resourceConf?.memory.request || DEFAULT_MEMORY_REQUEST,
                                },
                                limits: {
                                    "cpu": input.resourceConf?.cpu.limit || DEFAULT_CPU_LIMIT,
                                    "memory": input.resourceConf?.memory.limit || DEFAULT_MEMORY_LIMIT,
                                }
                            },
                            volumeMounts: [
                                {
                                    name: "badgerdb",
                                    mountPath: "/oxide",
                                }
                            ],
                        },
                    ],
                },
            },
            volumeClaimTemplates: [
                {
                    metadata: {
                        name: "badgerdb",
                    },
                    spec: {
                        accessModes: ["ReadWriteOnce"],
                        // if the storage class is undefined, default storage class is used by the PVC.
                        storageClassName: input.storageClass,
                        resources: {
                            requests: {
                                storage: `${input.storageCapacityGB}Gi`,
                            },
                        }
                    }
                }
            ]
        },
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    // Create backup service and statefulset for Nitrous.
    createBackupService(input, k8sProvider, imageName, bucketName)

    const output: outputType = {
        appLabels: appLabels,
        svc: nitrousSvc,
    }
    return output
}
