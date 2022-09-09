import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";
import * as aws from "@pulumi/aws";

import {topicConf} from "../kafkatopics";

export const plugins = {
    "aws": "v4.38.1",
    "kubernetes": "v3.20.1",
}

const DEFAULT_REPLICAS = 1
const DEFAULT_SOURCE_CONNECTOR_TASKS = 10
const DEFAULT_CHECKPOINT_CONNECTOR_TASKS = 10
const DEFAULT_MEMORY_REQUESTS = "1Gi"
const DEFAULT_MEMORY_LIMITS = "4Gi"
const DEFAULT_CPU_REQUESTS = "1"
const DEFAULT_CPU_LIMITS = "2"

export type MirrorMakerConf = {
    // increases the number of worker nodes running the "Tasks"
    replicas?: number,
    // this should ideally match the number of total partitions the topics to be mirrored have
    sourceConnectorTasks?: number,
    // this should ideally match the number of consumer groups the topics to be mirrored have
    checkpointConnectorTasks?: number,

    // worker nodes resource requirements
    cpuRequests?: string,
    cpuLimits?: string,
    memoryRequests?: string,
    memoryLimits?: string,
}

export type inputType = {
    tierId: number,
    roleArn: pulumi.Input<string>,
    region: string,
    kubeconfig: string,

    topics: topicConf[],
    conf: MirrorMakerConf,

    mskPassword: string,
    mskUsername: string,
    mskBootstrapServers: string,

    confluentPassword: pulumi.Output<string>,
    confluentUsername: string,
    confluentBootstrapServers: string,
}

// should not contain any pulumi.Output<> types.
export type outputType = {}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const awsProvider = new aws.Provider("mirror-maker-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const k8sProvider = new k8s.Provider("mirror-maker-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: "strimzi"
    });

    // secret with passwords
    const mirrorMakerCreds = new k8s.core.v1.Secret("mirror-maker-creds", {
        stringData: {
            "confluentPassword": input.confluentPassword,
            "mskPassword": input.mskPassword,
        },
        metadata: {
            name: `t-${input.tierId}-mirrormaker-passwords`
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true });

    let topicNames: string[] = [];
    input.topics.forEach((topicConf) => {
         topicNames.push(topicConf.name);
    })
    const topicRegex = topicNames.join("|");

    // setup the custom resource
    const mapping = new k8s.apiextensions.CustomResource("mirror-maker2-crd", {
        apiVersion: "kafka.strimzi.io/v1beta2",
        kind: "KafkaMirrorMaker2",
        metadata: {
            name: `t-${input.tierId}-mirrormaker2`,
        },
        spec: {
            "version": "3.2.0",
            "replicas": input.conf.replicas || DEFAULT_REPLICAS,
            "connectCluster": "target-cluster",
            "clusters": [
                {
                    "alias": "source-cluster",
                    "authentication": {
                        "type": "plain",
                        "username": input.confluentUsername,
                        "passwordSecret": {
                            "password": "confluentPassword",
                            "secretName": mirrorMakerCreds.metadata.name,
                        }
                    },
                    "bootstrapServers": input.confluentBootstrapServers,
                    "tls": {
                        "trustedCertificates": []
                    }
                },
                {
                    "alias": "target-cluster",
                    "authentication": {
                        "type": "scram-sha-512",
                        "username": input.mskUsername,
                        "passwordSecret": {
                            "password": "mskPassword",
                            "secretName": mirrorMakerCreds.metadata.name,
                        }
                    },
                    "bootstrapServers": input.mskBootstrapServers,
                    "config": {
                        "config.storage.replication.factor": 2,
                        "offset.storage.replication.factor": 2,
                        "status.storage.replication.factor": 2,
                    },
                    "tls": {
                        "trustedCertificates": []
                    }
                }
            ],
            "mirrors": [
                {
                    "sourceCluster": "source-cluster",
                    "targetCluster": "target-cluster",
                    "sourceConnector": {
                        "tasksMax": input.conf.sourceConnectorTasks || DEFAULT_SOURCE_CONNECTOR_TASKS,
                        "config": {
                            "replication.factor": 2,
                            "offset-syncs.topic.replication.factor": 2,
                            "replication.policy.class": "io.strimzi.kafka.connect.mirror.IdentityReplicationPolicy",
                            "offset-syncs.topic.location": "target",
                        }
                    },
                    "heartbeatConnector": {
                        "config": {
                            "heartbeats.topic.replication.factor": 2,
                        }
                    },
                    "checkpointConnector": {
                        "tasksMax": input.conf.checkpointConnectorTasks || DEFAULT_CHECKPOINT_CONNECTOR_TASKS,
                        "config": {
                            "checkpoints.topic.replication.factor": 2,
                            "sync.group.offsets.enabled": true,
                            "sync.group.offsets.interval.seconds": 10,
                            "emit.checkpoints.interval.seconds": 10,
                            "replication.policy.class": "io.strimzi.kafka.connect.mirror.IdentityReplicationPolicy",
                            "offset-syncs.topic.location": "target",
                        }
                    },
                    "topicsPattern": topicRegex,
                    // copy all of their consumer groups
                    "groupsPattern": ".*",
                }
            ],
            "resources": {
                "requests": {
                    "cpu": input.conf.cpuRequests || DEFAULT_CPU_REQUESTS,
                    "memory": input.conf.memoryRequests || DEFAULT_MEMORY_REQUESTS,
                },
                "limits": {
                    "cpu": input.conf.cpuLimits || DEFAULT_CPU_LIMITS,
                    "memory": input.conf.memoryLimits || DEFAULT_MEMORY_LIMITS,
                }
            },
            "template": {
                "pod": {
                    "affinity": {
                        "nodeAffinity": {
                            "requiredDuringSchedulingIgnoredDuringExecution": {
                                "nodeSelectorTerms": [
                                    {
                                        "matchExpressions": [
                                            {
                                                "key": "kubernetes.io/arch",
                                                "operator": "In",
                                                "values": [
                                                    "amd64"
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    "metadata": {
                        "annotations": {
                            "prometheus.io/scrape": true,
                            "prometheus.io/port": 9404,
                        }
                    }
                }
            },
            "metricsConfig": {
                "type": "jmxPrometheusExporter",
                "valueFrom": {
                    "configMapKeyRef": {
                        "name": "mirror-maker2-metrics",
                        "key": "metrics-config.yml",
                    }
                }
            }
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    const output = pulumi.output({})
    return output
}
