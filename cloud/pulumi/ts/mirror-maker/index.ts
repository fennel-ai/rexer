import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";
import * as aws from "@pulumi/aws";

import {topicConf} from "../kafkatopics";

export const plugins = {
    "aws": "v5.0.0",
    "kubernetes": "v3.20.1",
}

const DEFAULT_REPLICAS = 1
const DEFAULT_SOURCE_CONNECTOR_TASKS = 10
const DEFAULT_CHECKPOINT_CONNECTOR_TASKS = 10
const DEFAULT_MEMORY_REQUESTS = "20Gi"
const DEFAULT_MEMORY_LIMITS = "32Gi"
const DEFAULT_CPU_REQUESTS = "3"
const DEFAULT_CPU_LIMITS = "4"

export type inputType = {
    planeId: number,
    roleArn: pulumi.Input<string>,
    region: string,
    kubeconfig: any,

    sourcePassword: string,
    sourceUsername: string,
    sourceBootstrapServers: string,

    targetPassword: string,
    targetUsername: string,
    targetBootstrapServers: string,
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
            "sourcePassword": input.sourcePassword,
            "targetPassword": input.targetPassword,
        },
        metadata: {
            name: `p-${input.planeId}-mirrormaker-passwords`
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true });

    // setup the custom resource
    const mapping = new k8s.apiextensions.CustomResource("mirror-maker2-crd", {
        apiVersion: "kafka.strimzi.io/v1beta2",
        kind: "KafkaMirrorMaker2",
        metadata: {
            name: `p-${input.planeId}-mirrormaker2`,
        },
        spec: {
            "version": "3.2.0",
            "replicas": DEFAULT_REPLICAS,
            "connectCluster": "target-cluster",
            "clusters": [
                {
                    "alias": "source-cluster",
                    "authentication": {
                        "type": "scram-sha-512",
                        "username": input.sourceUsername,
                        "passwordSecret": {
                            "password": "sourcePassword",
                            "secretName": mirrorMakerCreds.metadata.name,
                        }
                    },
                    "bootstrapServers": input.sourceBootstrapServers,
                    "tls": {
                        "trustedCertificates": []
                    }
                },
                {
                    "alias": "target-cluster",
                    "authentication": {
                        "type": "scram-sha-512",
                        "username": input.targetUsername,
                        "passwordSecret": {
                            "password": "targetPassword",
                            "secretName": mirrorMakerCreds.metadata.name,
                        }
                    },
                    "bootstrapServers": input.targetBootstrapServers,
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
                        "tasksMax": DEFAULT_SOURCE_CONNECTOR_TASKS,
                        "config": {
                            "replication.factor": 2,
                            "offset-syncs.topic.replication.factor": 2,
                            "replication.policy.class": "io.strimzi.kafka.connect.mirror.IdentityReplicationPolicy",
                            "offset-syncs.topic.location": "target",
                            "sync.topic.acls.enabled": false,
                        }
                    },
                    "heartbeatConnector": {
                        "config": {
                            "heartbeats.topic.replication.factor": 2,
                        }
                    },
                    "checkpointConnector": {
                        "tasksMax": DEFAULT_CHECKPOINT_CONNECTOR_TASKS,
                        "config": {
                            "checkpoints.topic.replication.factor": 2,
                            "sync.group.offsets.enabled": true,
                            "sync.group.offsets.interval.seconds": 10,
                            "emit.checkpoints.interval.seconds": 10,
                            "replication.policy.class": "io.strimzi.kafka.connect.mirror.IdentityReplicationPolicy",
                            "offset-syncs.topic.location": "target",
                        }
                    },

                    "topicsPattern": "t_107_actionlog|t_107_profilelog|p_5_nitrous_log|p_5_aggregates_conf",
                    // sync consumer group offsets for everything except nitrous
                    "groupsPattern": "^(?!nitrous-0|nitrous-1|nitrous-backup-0).*",
                }
            ],
            "resources": {
                "requests": {
                    "cpu": DEFAULT_CPU_REQUESTS,
                    "memory": DEFAULT_MEMORY_REQUESTS,
                },
                "limits": {
                    "cpu": DEFAULT_CPU_LIMITS,
                    "memory": DEFAULT_MEMORY_LIMITS,
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
    }, { provider: k8sProvider, replaceOnChanges: ["*"],  deleteBeforeReplace: true })

    const output = pulumi.output({})
    return output
}
