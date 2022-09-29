import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws"
import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as path from "path";
import * as kafka from "@pulumi/kafka";
import * as process from "process";
import * as childProcess from "child_process";
import * as util from "../lib/util";
import { ReadinessProbe } from "../tier-consts/consts";
import { INSTANCE_METADATA_SERVICE_ADDR } from "../lib/util";


export const plugins = {
    "aws": "v5.1.0",
    "kubernetes": "v3.20.1",
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
const NITROUS_REQS_TOPIC_NAME = "nitrous_req_log"

// default for resource requirement configurations
const DEFAULT_CPU_REQUEST = "1000m"
const DEFAULT_CPU_LIMIT = "1500m"
const DEFAULT_MEMORY_REQUEST = "2Gi"
const DEFAULT_MEMORY_LIMIT = "4Gi"

export const name = "nitrous"
export const namespace = "fennel"
export const servicePort = 3333;

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
    // The largest record batch size allowed by Kafka
    max_message_bytes?: number,
}

export type kafkaAdmin = {
    username: pulumi.Output<string>,
    password: pulumi.Output<string>,
    bootstrapServers: pulumi.Output<string>,
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
    svc: pulumi.Output<k8s.core.v1.Service>,
    binlogPartitions: number,
}

function setupBinLogInMsk(input: inputType, binlogPartitions: number, awsProvider: aws.Provider) {
    // currently TLS is disabled
    const bootstrapServers = input.kafka.bootstrapServers.apply(bootstrapServers => { return bootstrapServers.split(","); })
    const kafkaProvider = new kafka.Provider("nitrous-kafka-provider-msk", {
        // bootstrap servers is a string with comma separated server addresses
        bootstrapServers: bootstrapServers,
        saslUsername: input.kafka.username,
        saslPassword: input.kafka.password,
        saslMechanism: "scram-sha512",
    }, { provider: awsProvider });
    const config = {
        "retention.ms": input.binlog.retention_ms,
        "retention.bytes": input.binlog.partition_retention_bytes,
        "max.message.bytes": input.binlog.max_message_bytes,
    };
    const topic = new kafka.Topic(`topic-p-${input.planeId}-${BINLOG_TOPIC_NAME}-msk`, {
        name: `p_${input.planeId}_${BINLOG_TOPIC_NAME}`,
        partitions: binlogPartitions,
        // We set a default replication factor of 2 (has to be > 1 since this could block producers during a
        // rolling update a broker could be brought down). For production workloads we expect this value to be >= 3
        //
        // since we configure 2 AZs, setting replication factor as 2 is fine for non-production workloads,
        // but it could cause potential partial data loss - this is possible when the "leader" replica for a partition
        // is down and one of the AZ is unreachable, kafka control plane is unable to assign a broker as the leader,
        // which causes the new messages to be dropped
        replicationFactor: input.binlog.replicationFactor || 2,
        config: config,
    }, { provider: kafkaProvider, protect: input.protect })
    const reqTopic = new kafka.Topic(`topic-p-${input.planeId}-${NITROUS_REQS_TOPIC_NAME}-msk`, {
        name: `p_${input.planeId}_${NITROUS_REQS_TOPIC_NAME}`,
        partitions: binlogPartitions,
        // We set a default replication factor of 2 (has to be > 1 since this could block producers during a
        // rolling update a broker could be brought down). For production workloads we expect this value to be >= 3
        //
        // since we configure 2 AZs, setting replication factor as 2 is fine for non-production workloads,
        // but it could cause potential partial data loss - this is possible when the "leader" replica for a partition
        // is down and one of the AZ is unreachable, kafka control plane is unable to assign a broker as the leader,
        // which causes the new messages to be dropped
        replicationFactor: input.binlog.replicationFactor || 2,
        config: config,
    }, { provider: kafkaProvider });

    // create a partition for aggregate configuration events
    const aggrConfTopic = new kafka.Topic(`topic-p-${input.planeId}-aggrConf`, {
        name: `p_${input.planeId}_aggregates_conf`,
        // create a single partition, since the ordering guarantees are a must for aggregate configurations
        // (e.g. seeing an aggregate deletion before creation might leave the system in a bad state).
        partitions: 1,
        replicationFactor: 2,
        config: {
            // set unlimited retention since aggregate configurations are required forever
            "retention.ms": -1,
        },
    }, { provider: kafkaProvider });

    const k8sProvider = new k8s.Provider("configs-k8s-provider-msk", {
        kubeconfig: input.kubeconfig,
        namespace: namespace,
    })
    return new k8s.core.v1.Secret("kafka-config-msk", {
        stringData: {
            "servers": input.kafka.bootstrapServers,
            "username": input.kafka.username,
            "password": input.kafka.password,
        },
        metadata: {
            name: "kafka-conf-msk",
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true })
}

export const setup = async (input: inputType) => {
    const awsProvider = new aws.Provider("nitrous-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })
    const binlogPartitions = input.binlog.partitions || DEFAULT_BINLOG_PARTITIONS;

    // Setup binlog kafka topic.
    const mskCreds = setupBinLogInMsk(input, binlogPartitions, awsProvider);

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

    const root = process.env["FENNEL_ROOT"]!
    // Get the (hash) commit id.
    // NOTE: This requires git to be installed and DOES NOT take local changes or commits into consideration.
    const hashId = childProcess.execSync('git rev-parse --short HEAD').toString().trim()
    const imageName = repo.repositoryUrl.apply(imgName => {
        return `${imgName}:${hashId}`
    })

    // Build and publish the container image.
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
    // we should schedule all components of Nitrous on ON_DEMAND instances
    nodeSelector["eks.amazonaws.com/capacityType"] = "ON_DEMAND";

    const image = new docker.Image("nitrous-img", {
        build: {
            context: root,
            dockerfile: dockerfile,
            args: {
                "platform": platform,
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
    const healthPort = 8082;

    const forceReplicaIsolation = input.enforceReplicaIsolation || DEFAULT_FORCE_REPLICA_ISOLATION;
    let whenUnsatisfiable = "ScheduleAnyway";
    if (forceReplicaIsolation) {
        whenUnsatisfiable = "DoNotSchedule";
    }

    if (input.storageClass === undefined) {
        console.log('storageClass is undefined for Nitrous - will use default storage class for persistent volume.')
    }

    const memlimit = input.resourceConf?.memory.limit || DEFAULT_MEMORY_LIMIT

    const appStatefulset = image.imageName.apply(() => {
        return new k8s.apps.v1.StatefulSet("nitrous-statefulset", {
            metadata: {
                name: "nitrous",
                labels: appLabels,
            },
            spec: {
                serviceName: "nitrous",
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
                            "prometheus.io/scrape_frequent": "true",
                            "prometheus.io/port": metricsPort.toString(),
                        }
                    },
                    spec: {
                        nodeSelector: nodeSelector,
                        // We want to set the vm.swappniss sysctl to 0, but that
                        // is blocked on https://github.com/pulumi/pulumi-eks/issues/611
                        // Once the above issue resolved, we should add the following
                        // as an option to the managed nodegroup for nitrous:
                        //     kubeletExtraArgs: "--allowed-unsafe-sysctls=vm.swappiness",
                        // After that, we can set the vm.swappiness sysctl to 1
                        // for this pod by uncommenting the following:
                        // securityContext: {
                        //     sysctls: [
                        //         {
                        //             name: "vm.swappiness",
                        //             value: "1",
                        //         }
                        //     ],
                        // },
                        containers: [
                            {
                                command: [
                                    "/root/nitrous",
                                    "--listen-port",
                                    `${servicePort}`,
                                    "--metrics-port",
                                    `${metricsPort}`,
                                    "--health-port",
                                    `${healthPort}`,
                                    "--plane-id",
                                    `${input.planeId}`,
                                    "--gravel_dir",
                                    "/oxide/gravel_singleshard",
                                    "--badger_block_cache_mb",
                                    `${input.blockCacheMB}`,
                                    "--ristretto_max_cost",
                                    (input.kvCacheMB * 1024 * 1024).toString(),
                                    "--otlp-endpoint",
                                    input.otlpEndpoint,
                                    "--dev=false"
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
                                    {
                                        containerPort: healthPort,
                                        protocol: "TCP",
                                    },
                                ],
                                env: [
                                    {
                                        name: "MSK_KAFKA_SERVER_ADDRESS",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "kafka-conf-msk",
                                                key: "servers",
                                            }
                                        }
                                    },
                                    {
                                        name: "MSK_KAFKA_USERNAME",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "kafka-conf-msk",
                                                key: "username",
                                            }
                                        }
                                    },
                                    {
                                        name: "MSK_KAFKA_PASSWORD",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "kafka-conf-msk",
                                                key: "password",
                                            }
                                        }
                                    },
                                    {
                                        name: "GOMEMLIMIT",
                                        value: memlimit + "B",
                                    },
                                    {
                                        name: "OTEL_SERVICE_NAME",
                                        value: "nitrous",
                                    },
                                    {
                                        name: "BINLOG_PARTITIONS",
                                        value: `${binlogPartitions}`
                                    },
                                    {
                                        name: "IDENTITY",
                                        valueFrom: {
                                            fieldRef: {
                                                fieldPath: "metadata.labels['statefulset.kubernetes.io/pod-name']",
                                            }
                                        }
                                    },
                                    {
                                        name: "JE_MALLOC_CONF",
                                        value: "background_thread:true,metadata_thp:auto"
                                    },
                                    {
                                        name: "INSTANCE_METADATA_SERVICE_ADDR",
                                        value: INSTANCE_METADATA_SERVICE_ADDR
                                    }
                                ],
                                resources: {
                                    requests: {
                                        "cpu": input.resourceConf?.cpu.request || DEFAULT_CPU_REQUEST,
                                        "memory": input.resourceConf?.memory.request || DEFAULT_MEMORY_REQUEST,
                                    },
                                    limits: {
                                        "memory": memlimit,
                                    }
                                },
                                readinessProbe: ReadinessProbe(healthPort),
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
                ],
                // default update strategy is "RollingUpdate" with "maxUnavailable: 1". Stateful sets have a
                // concept of partitions, but I believe are useful for canary rollout
            },
        }, { provider: k8sProvider, deleteBeforeReplace: true, dependsOn: [mskCreds] });
    })

    const appSvc = appStatefulset.apply(() => {
        return new k8s.core.v1.Service("nitrous-svc", {
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
    })

    const output: outputType = {
        appLabels: appLabels,
        svc: appSvc,
        binlogPartitions: binlogPartitions
    }
    return output
}
