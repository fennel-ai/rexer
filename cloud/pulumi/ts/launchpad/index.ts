import setupTier, { TierConf, TierMskConf } from "./tier";
import setupDataPlane, { DataPlaneConf, PlaneOutput } from "./data_plane";
import setupMothership, { MothershipConf } from "./mothership";
import * as vpc from "../vpc";
import * as eks from "../eks";
import * as account from "../account";
import * as aurora from "../aurora";
import * as postgres from "../postgres";
import * as elasticache from "../elasticache";
import * as redis from "../redis";
import * as connsink from "../connectorsink";
import * as offlineAggregateSource from "../offline-aggregate-script-source";
import * as glueSource from "../glue-script-source";
import * as kafkatopics from "../kafkatopics";
import * as telemetry from "../telemetry";
import * as nitrous from "../nitrous";
import * as milvus from "../milvus";
import { nameof, Plan } from "../lib/util";
import * as msk from "../msk";

import { DEFAULT_ARM_AMI_TYPE, DEFAULT_X86_AMI_TYPE, ON_DEMAND_INSTANCE_TYPE, SPOT_INSTANCE_TYPE } from "../eks";
import { OutputMap } from "@pulumi/pulumi/automation";
import { MothershipDBUpdater, Customer } from "../mothership-updates"

const controlPlane: vpc.controlPlaneConfig = {
    region: "us-west-2",
    accountId: "030813887342",
    vpcId: "vpc-0d9942e83f94c049c",
    roleArn: account.MASTER_ACCOUNT_ADMIN_ROLE_ARN,
    routeTableId: "rtb-07afe7458db9c4479",
    cidrBlock: "172.31.0.0/16",
    primaryPrivateSubnet: "subnet-07aa4b44ebd42517e",
    secondaryPrivateSubnet: "subnet-091ccb4e147da9859",
    primaryPublicSubnet: "subnet-00801991ba653e52c",
    secondaryPublicSubnet: "subnet-0f3a7cbfd18588331",
}

const customers: Record<number, Customer> = {
    1: {
        id: 1,
        domain: "fennel.ai",
        name: "self-serve",
    },
    2: {
        id: 2,
        domain: "convoynetwork.com",
        name: "convoy",
    },
    3: {
        id: 3,
        domain: "getlokalapp.com",
        name: "lokal",
    },
    4: {
        id: 5,
        domain: "yext.com",
        name: "Yext",
    }
};
//================ Static data plane / tier configurations =====================

// map from tier id to plane id.
const tierConfs: Record<number, TierConf> = {
    // Fennel staging tier using Fennel's staging data plane.
    108: {
        protectResources: true,
        planeId: 3,
        tierId: 108,
        ingressConf: {
            usePublicSubnets: true,
        },
        // enable separate query server svc.
        queryServerConf: {},
        enableOfflineAggregationJobs: true,
        // NOTE: We make the airbyte instance hosted on the staging tier public for unit, integration and e2e tests
        //
        // Since Airbyte is a tier level resource, our testing plane 2, does not have any tiers on it. Hence we will
        // use this tier for all purposes for now
        //
        // TODO(mohit): Consider introducing a test tier if required on the plane 2 - since we now have sagemaker tests
        // requiring setting up an tier-level endpoint etc, we currently work on manually created test endpoints
        // it might be better to create a test tier and use it's resources instead if more such requirements arise
        // in the future
        airbyteConf: {
            publicServer: true,
        },
    },
    // Convoy prod tier
    112: {
        protectResources: true,
        planeId: 9,
        tierId: 112,
        // TODO(mohit): set service configurations
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
        },

        // they only use the offline aggregation
        enableOfflineAggregationJobs: true,

        plan: Plan.STARTUP,
        requestLimit: 0,
    },
    116: {
        protectResources: true,
        planeId: 14,
        tierId: 116,
        // set larger requests for the http + query server
        httpServerConf: {
            podConf: {
                minReplicas: 1,
                maxReplicas: 4,
                resourceConf: {
                    cpu: {
                        request: "1000m",
                        limit: "2000m"
                    },
                    memory: {
                        request: "3G",
                        limit: "4G",
                    }
                }
            },
        },
        // Yext currently only works with offline aggregation jobs.
        enableOfflineAggregationJobs: true,
        plan: Plan.STARTUP,
        requestLimit: 0,
        enableCors: true,
    },
    // Lokal prod tier in their account.
    130: {
        protectResources: true,
        // this is the plane in the new account for lokal
        planeId: 13,
        // assign the same tier id as the original account - this is to have the same db names and kafka topics
        // which are being copied/mirrored from the original account.
        tierId: 107,
        // this is required for any resource in the global namespace e.g. pulumi stack, s3 buckets, etc.
        tierName: "lokal-org-prod-tier",

        // training data generation is required for model training
        enableTrainingDatasetGenerationJobs: true,

        httpServerConf: {
            podConf: {
                minReplicas: 2,
                maxReplicas: 4,
                resourceConf: {
                    cpu: {
                        request: "1000m",
                        limit: "1500m"
                    },
                    memory: {
                        request: "2G",
                        limit: "3G",
                    }
                },
                // each http-server should be in different nodes from each other
                nodeLabels: {
                    "node-group": "p-5-httpserver-ng"
                }
            }
        },

        // countaggr should be scheduled in a different node than http-server
        countAggrConf: {
            podConf: {
                nodeLabels: {
                    "node-group": "p-5-countaggr-ng"
                },
                resourceConf: {
                    // c6g.2xlarge machine, set requests and limits accordingly
                    cpu: {
                        request: "6000m",
                        limit: "8000m",
                    },
                    memory: {
                        request: "10Gi",
                        limit: "14Gi",
                    }
                }
            }
        },

        // TODO(mohit): Currently the requests are configured such that each replica is going to be scheduled
        // in different node in the node group, ideally we should try to reduce the `request` and let the scheduler
        // place the pods across the nodes based on utilization and `limit`
        queryServerConf: {
            podConf: {
                minReplicas: 2,
                maxReplicas: 10,
                resourceConf: {
                    // c6g.xlarge machines, set requests and limits accordingly
                    cpu: {
                        request: "2500m",
                        limit: "4000m"
                    },
                    memory: {
                        request: "5G",
                        limit: "7G",
                    }
                },
                nodeLabels: {
                    "node-group": "p-5-queryserver-ng"
                },
            }
        },

        sagemakerConf: {
            // this is the cheapest sagemaker instance type other than burstable instances (t3, t4g.. - but they are
            // not autoscalable).
            instanceType: "ml.m5.large",
            instanceCount: 1,
        },
        ingressConf: {
            useDedicatedMachines: true,
            replicas: 3,
        },
        airbyteConf: {},
        plan: Plan.STARTUP,
        requestLimit: 0,
    }
}

// map from plane id to its configuration.
const dataPlaneConfs: Record<number, DataPlaneConf> = {
    // Fennel's staging data plane to run dev tiers
    3: {
        protectResources: true,

        accountConf: {
            existingAccount: {
                roleArn: account.MASTER_ACCOUNT_ADMIN_ROLE_ARN,
            }
        },

        planeId: 3,
        region: "us-west-2",
        vpcConf: {
            cidr: "10.103.0.0/16"
        },
        dbConf: {
            minCapacity: 1,
            maxCapacity: 4,
            password: "foundationdb",
            skipFinalSnapshot: true,
        },
        controlPlaneConf: controlPlane,
        redisConf: {
            numShards: 4,
            nodeType: "db.t4g.medium",
            numReplicasPerShard: 0,
        },
        prometheusConf: {
            volumeSizeGiB: 128,
            metricsRetentionDays: 60,
        },
        cacheConf: {
            // this is used for demo tiers, which could right a lot of profiles
            numNodeGroups: 2,
            nodeType: "cache.t4g.medium",
            replicasPerNodeGroup: 0,
        },
        // increase the desired capacity and scale up to occupy more pods
        //
        // https://github.com/awslabs/amazon-eks-ami/blob/master/files/eni-max-pods.txt
        eksConf: {
            nodeGroups: [
                {
                    name: "p-3-common-ng-arm64-new",
                    instanceTypes: ["t4g.medium"],
                    minSize: 4,
                    // since we create demo tiers on top of this plane, allow scaling this node group to a larger
                    // number to accommodate more servers
                    maxSize: 10,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                {
                    name: "p-3-common-ng-x86-new",
                    instanceTypes: ["t3.medium"],
                    minSize: 3,
                    maxSize: 10,
                    amiType: DEFAULT_X86_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                // Nitrous node group.
                {
                    name: "p-3-nitrous-ng-arm",
                    instanceTypes: ["c6gd.large"],
                    minSize: 1,
                    maxSize: 1,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    labels: {
                        "node-group": "p-3-nitrous-ng",
                        "aws.amazon.com/eks-local-ssd": "true",
                    },
                    expansionPriority: 1,
                },
                // Nitrous backup node group
                //
                // TODO(mohit): See if it is possible to scale up the existing nitrous node group whenever the backup
                // pod requires a node to be up and running.
                // It seems like the cluster autoscaler does not work well with pods being in pending state due to
                // lack of persistent volume claims, especially with local SSDs (since they don't have a CSI driver)
                // - https://github.com/kubernetes/autoscaler/issues/1658
                {
                    name: "p-3-nitrous-backup-ng-arm",
                    instanceTypes: ["c6gd.large"],
                    minSize: 1,
                    maxSize: 1,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    labels: {
                        "node-group": "p-3-nitrous-backup-ng",
                        "aws.amazon.com/eks-local-ssd": "true",
                    },
                    expansionPriority: 1,
                },
            ],
        },
        // milvusConf: {},
        // modelMonitoringConf: {},
        // Run nitrous on the plane.
        nitrousConf: {
            replicas: 1,
            storageCapacityGB: 100,
            storageClass: "local",
            binlog: {
                partitions: 10,
            },
            nodeLabels: {
                "node-group": "p-3-nitrous-ng",
            },

            forceLoadBackup: true,

            // backup configurations
            backupConf: {
                nodeLabelsForBackup: {
                    "node-group": "p-3-nitrous-backup-ng",
                },
                backupFrequencyDuration: "5m",
                remoteCopiesToKeep: 2,
                // using the same node type as the primary nitrous instances
                storageCapacityGB: 100,
            },
        },

        // set up MSK cluster
        mskConf: {
            brokerType: "kafka.m5.large",
            // this will place 1 broker node in each of the AZs
            numberOfBrokerNodes: 2,
            // consider expanding this in the future if each broker needs more storage capacity
            storageVolumeSizeGiB: 128,
        },
    },
    // plane 8 - pending account close, post which it can be destroyed
    // Convoy's production plane
    9: {
        protectResources: true,

        accountConf: {
            newAccount: {
                name: "convoy",
                email: "admin+convoy@fennel.ai",
            }
        },

        planeId: 9,
        region: "us-west-2",
        vpcConf: {
            cidr: "10.109.0.0/16"
        },
        eksConf: {
            nodeGroups: [
                {
                    name: "p-9-common-ng-x86",
                    instanceTypes: ["t3.medium"],
                    minSize: 1,
                    maxSize: 5,
                    amiType: DEFAULT_X86_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                {
                    name: "p-9-common-ng-arm64",
                    instanceTypes: ["t4g.medium"],
                    minSize: 1,
                    maxSize: 5,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                // Convoy does not use nitrous, we setup a basic nitrous cluster with no backups
                // Nitrous node group.
                {
                    name: "p-9-nitrous-ng-arm",
                    instanceTypes: ["m6gd.medium"],
                    minSize: 1,
                    maxSize: 1,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    labels: {
                        "node-group": "p-9-nitrous-ng",
                        "aws.amazon.com/eks-local-ssd": "true",
                    },
                    expansionPriority: 1,
                },
            ],
        },
        dbConf: {
            minCapacity: 1,
            maxCapacity: 4,
            password: "password",
            skipFinalSnapshot: true,
        },
        controlPlaneConf: controlPlane,
        redisConf: {
            numShards: 1,
            nodeType: "db.t4g.medium",
            numReplicasPerShard: 1,
        },
        cacheConf: {
            nodeType: "cache.t4g.small",
            numNodeGroups: 1,
            replicasPerNodeGroup: 1,
        },
        prometheusConf: {
            volumeSizeGiB: 256,
            metricsRetentionDays: 60,
        },
        // set up MSK cluster
        mskConf: {
            // compute cost = 0.21 ($/hr) x 2 (#brokers) x 720 = ~$300
            brokerType: "kafka.m5.large",
            // this will place 1 broker node in each of the AZs
            numberOfBrokerNodes: 2,
            // storage cost = 0.10 ($/GB-month) x 64 = 6.4$
            storageVolumeSizeGiB: 64,
        },
        // Convoy does not use nitrous, we setup a basic nitrous cluster.
        nitrousConf: {
            replicas: 1,
            useAmd64: false,
            storageCapacityGB: 50,
            storageClass: "local",
            resourceConf: {
                cpu: {
                    request: "200m",
                    limit: "2000m"
                },
                memory: {
                    request: "1Gi",
                    limit: "1200Mi",
                }
            },
            binlog: {
                partitions: 16,
                retention_ms: 30 * 24 * 60 * 60 * 1000,  // 30 days
                partition_retention_bytes: -1,
                max_message_bytes: 2097164,
                replicationFactor: 2,
            },
            nodeLabels: {
                "node-group": "p-9-nitrous-ng",
            },
        },
        customer: customers[2],
        mothershipId: 12,
    },
    // plane 10 - for self serve pending account close
    // Yext's production plane
    14: {
        protectResources: true,
        accountConf: {
            existingAccount: {
                roleArn: "arn:aws:iam::893589383464:role/blood_orange",
            }
        },
        planeId: 14,
        region: "us-east-1",
        // Select AZs that support memorydb:
        // https://docs.aws.amazon.com/memorydb/latest/devguide/subnetgroups.html.
        // Note that different accounts will have different az names
        // that support MemoryDB.
        azs: ["us-east-1a", "us-east-1c"],
        vpcConf: {
            cidr: "10.114.0.0/16"
        },
        eksConf: {
            nodeGroups: [
                {
                    name: "p-14-common-ng-x86",
                    instanceTypes: ["t3.medium"],
                    minSize: 1,
                    maxSize: 3,
                    amiType: DEFAULT_X86_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                {
                    name: "p-14-common-ng-arm64",
                    instanceTypes: ["t4g.medium"],
                    minSize: 2,
                    maxSize: 4,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                // Nitrous node group.
                {
                    name: "p-14-nitrous-ng-arm",
                    instanceTypes: ["c6gd.large"],
                    minSize: 1,
                    maxSize: 1,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    labels: {
                        "node-group": "p-14-nitrous-ng",
                        "aws.amazon.com/eks-local-ssd": "true",
                    },
                    expansionPriority: 1,
                },
                // Nitrous backup node group
                {
                    name: "p-14-nitrous-backup-ng-arm",
                    instanceTypes: ["c6gd.large"],
                    minSize: 1,
                    maxSize: 1,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    labels: {
                        "node-group": "p-14-nitrous-backup-ng",
                        "aws.amazon.com/eks-local-ssd": "true",
                    },
                    expansionPriority: 1,
                },
            ],
        },
        dbConf: {
            minCapacity: 1,
            maxCapacity: 4,
            password: "password",
            skipFinalSnapshot: true,
        },
        controlPlaneConf: controlPlane,
        redisConf: {
            numShards: 1,
            nodeType: "db.t4g.medium",
            numReplicasPerShard: 1,
        },
        cacheConf: {
            nodeType: "cache.t4g.small",
            numNodeGroups: 1,
            replicasPerNodeGroup: 1,
        },
        prometheusConf: {
            volumeSizeGiB: 256,
            metricsRetentionDays: 60,
        },
        nitrousConf: {
            replicas: 1,
            useAmd64: false,
            storageCapacityGB: 50,
            storageClass: "local",
            resourceConf: {
                cpu: {
                    request: "1000m",
                    limit: "2000m"
                },
                memory: {
                    request: "1Gi",
                    limit: "1200Mi",
                }
            },
            binlog: {
                partitions: 16,
                retention_ms: 30 * 24 * 60 * 60 * 1000,  // 30 days
                partition_retention_bytes: -1,
                max_message_bytes: 2097164,
                replicationFactor: 2,
            },
            nodeLabels: {
                "node-group": "p-14-nitrous-ng",
            },

            // backup configurations
            backupConf: {
                nodeLabelsForBackup: {
                    "node-group": "p-14-nitrous-backup-ng",
                },
                backupFrequencyDuration: "60m",
                remoteCopiesToKeep: 2,
                resourceConf: {
                    cpu: {
                        request: "1000m",
                        limit: "2000m"
                    },
                    memory: {
                        request: "1Gi",
                        limit: "1200Mi",
                    }
                },
                storageCapacityGB: 50,
            },
        },
        // set up MSK cluster
        mskConf: {
            // compute cost = 0.21 ($/hr) x 2 (#brokers) x 720 = ~$300
            brokerType: "kafka.m5.large",
            // this will place 1 broker node in each of the AZs
            numberOfBrokerNodes: 2,
            // storage cost = 0.10 ($/GB-month) x 64 = 6.4$
            storageVolumeSizeGiB: 64,
        },
        customer: customers[4],
        mothershipId: 12,
    },
    // plane 11 - lokal plane in their organization, pending account close
    // Skipped 12 to avoid conflict with the mothership.
    13: {
        protectResources: true,
        accountConf: {
            existingAccount: {
                roleArn: "arn:aws:iam::611878335506:role/admin"
            }
        },
        planeName: "lokal-org-prod",
        planeId: 5,
        region: "ap-south-1",
        vpcConf: {
            cidr: "10.113.0.0/16"
        },
        dbConf: {
            minCapacity: 2,
            maxCapacity: 64,
            password: "password",
            skipFinalSnapshot: false,
        },
        cacheConf: {
            nodeType: "cache.t4g.medium",
            // use smaller number of cache nodes - this is required for profiles, we are almost always ~99.9%
            numNodeGroups: 2,
            replicasPerNodeGroup: 1,
        },
        controlPlaneConf: controlPlane,
        redisConf: {
            // keep 1 shard for the existing users of redis - phaser and action dedup check logic
            numShards: 1,
            // this is only required for actions and streamlog deduplication - currently with a `db.r6g.large` instance
            // the memory utilization is around 1%
            nodeType: "db.t4g.small",
            numReplicasPerShard: 1,
        },
        eksConf: {
            nodeGroups: [
                // HTTP server node group
                {
                    name: "p-5-httpserver-ng-arm64",
                    instanceTypes: ["t4g.medium"],
                    // at least have 2 nodes for fault tolerance
                    minSize: 2,
                    maxSize: 5,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    labels: {
                        "node-group": "p-5-httpserver-ng"
                    },
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                // Countaggr server node group
                {
                    name: "p-5-countaggr-ng-arm64",
                    // TODO(mohit): Move to c7g once they are supported in ap-south-1
                    instanceTypes: ["c6g.2xlarge"],
                    minSize: 1,
                    maxSize: 1,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    labels: {
                        "node-group": "p-5-countaggr-ng"
                    },
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },

                // Query server node groups
                {
                    name: "p-5-query-ng-arm",
                    // TODO(mohit): Move to c7g if/when they become cheaper than
                    // c6g instances in ap-south-1.
                    instanceTypes: ["c6g.xlarge"],
                    minSize: 1,
                    maxSize: 20,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    labels: {
                        "node-group": "p-5-queryserver-ng",
                        "rescheduler-label": "on-demand",
                    },
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                {
                    name: "p-5-query-ng-arm-spot",
                    // TODO(mohit): Move to c7g if/when they become cheaper than
                    // c6g instances in ap-south-1.
                    instanceTypes: ["c6g.xlarge", "c6gn.xlarge", "c6gd.xlarge"],
                    minSize: 1,
                    maxSize: 20,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    labels: {
                        "node-group": "p-5-queryserver-ng",
                        "rescheduler-label": "spot",
                    },
                    capacityType: SPOT_INSTANCE_TYPE,
                    expansionPriority: 10,
                },
                // Common node groups in case some container needs to be run on these
                {
                    name: "p-5-common-ng-arm64",
                    instanceTypes: ["t4g.medium"],
                    minSize: 1,
                    maxSize: 5,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                {
                    name: "p-5-common-ng-x86",
                    instanceTypes: ["t3.medium"],
                    minSize: 1,
                    maxSize: 5,
                    amiType: DEFAULT_X86_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },

                // Nitrous node group.
                {
                    name: "p-5-nitrous-4xl-ng-arm",
                    // 16vCpu, 64GiB and 900GB of local SSD
                    instanceTypes: ["m6gd.4xlarge"],
                    minSize: 2,
                    maxSize: 2,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    labels: {
                        "node-group": "p-5-nitrous-ng",
                        "aws.amazon.com/eks-local-ssd": "true",
                    },
                    expansionPriority: 1,
                },
                // Nitrous backup node group.
                {
                    name: "p-5-nitrous-backup-ng-arm",
                    // 8vCpu, 64GiB and 475GB of local SSD - $0.299
                    instanceTypes: ["r6gd.2xlarge"],
                    minSize: 1,
                    maxSize: 1,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    labels: {
                        "node-group": "p-5-nitrous-backup-ng",
                        "aws.amazon.com/eks-local-ssd": "true",
                    },
                    expansionPriority: 1,
                },
            ],
            spotReschedulerConf: {
                spotNodeLabel: "rescheduler-label=spot",
                onDemandNodeLabel: "rescheduler-label=on-demand",
            },
        },
        prometheusConf: {
            volumeSizeGiB: 256,
            metricsRetentionDays: 60,
        },

        // Run nitrous on the plane.
        nitrousConf: {
            replicas: 2,
            useAmd64: false,
            storageCapacityGB: 850,
            storageClass: "local",
            resourceConf: {
                cpu: {
                    request: "14500m",
                    limit: "16000m"
                },
                memory: {
                    request: "57Gi",
                    limit: "58Gi",
                }
            },
            binlog: {
                partitions: 32,
                retention_ms: 30 * 24 * 60 * 60 * 1000,  // 30 days
                partition_retention_bytes: -1,
                max_message_bytes: 2097164,
                replicationFactor: 2,
                // min in-sync replicas = 1
            },
            nodeLabels: {
                "node-group": "p-5-nitrous-ng",
            },

            // backup configurations
            backupConf: {
                nodeLabelsForBackup: {
                    "node-group": "p-5-nitrous-backup-ng",
                },
                backupFrequencyDuration: "60m",
                remoteCopiesToKeep: 2,
                // this needs to be consistent with the node group which this pod is going to get scheduled on
                //
                // currently r6gd.2xlarge
                resourceConf: {
                    cpu: {
                        request: "6000m",
                        limit: "8000m"
                    },
                    memory: {
                        request: "55Gi",
                        limit: "60Gi",
                    }
                },
                storageCapacityGB: 400,
            },
        },

        // set up MSK cluster
        mskConf: {
            // see - https://aws.amazon.com/msk/pricing/
            brokerType: "kafka.m5.large",
            // this will place 1 broker nodes in each of the AZs
            numberOfBrokerNodes: 2,
            storageVolumeSizeGiB: 1200,
        },
        customer: customers[3],
        mothershipId: 12,
    },
}

const mothershipConfs: Record<number, MothershipConf> = {
    // Control plane for prod.
    12: {
        protectResources: true,
        planeId: 12,
        vpcConf: controlPlane,
        dbConf: {
            minCapacity: 4,
            maxCapacity: 8,
            password: "foundationdb",
            skipFinalSnapshot: false,
        },
        ingressConf: {
            useDedicatedMachines: true,
            replicas: 3,
            usePublicSubnets: true,
        },
        eksConf: {
            nodeGroups: [
                {
                    name: "m-12-common-ng-x86",
                    instanceTypes: ["t3.medium"],
                    minSize: 1,
                    maxSize: 3,
                    amiType: DEFAULT_X86_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
            ],
        },
        bridgeServerConf: {
            podConf: {
                minReplicas: 1,
                maxReplicas: 3,
                resourceConf: {
                    cpu: {
                        request: "1250m",
                        limit: "1500m"
                    },
                    memory: {
                        request: "2G",
                        limit: "3G",
                    }
                },
                useAmd64: true,
            },
            envVars: [
                {
                    "name": "GIN_MODE",
                    "value": "release"
                },
                {
                    "name": "BRIDGE_SESSION_KEY",
                    "value": "a2ecf773ab9055f6c8af782bf606a495089b2e2f18636d3e3bd78804776fa529a80550359f48be67bcfa03e037ee90b1dc6bb389b32e3e54f0c87a6aaa77ac1b"
                }
            ],

        },
        dnsName: "app.fennel.ai",
    },
}

//==============================================================================

var preview = false;
var destroy = false;

// process.argv contains the whole command-line invocation.

// first 2 are meant for the shell that invoked the script.

// We assume that if 4 arguments are passed, 3rd signifies the action to perform and 4th argument signifies
// the "ID" (or stack) to act on. We support `preview` right now.
if (process.argv.length == 4) {
    const action = process.argv[process.argv.length - 2];
    if (action === "preview") {
        preview = true;
    } else if (action === "destroy") {
        destroy = true;
    } else {
        console.log(`${action} is not a supported action`)
        process.exit(1)
    }
}

const id = Number.parseInt(process.argv[process.argv.length - 1])
// TODO(Amit): This is becoming hard to maintain, think of a stack builder abstraction.

function getMothershipId(id: number | undefined): number | undefined {
    if (id === undefined || id in mothershipConfs) {
        return id
    }
    else if (id in dataPlaneConfs) {
        return getMothershipId(dataPlaneConfs[id].mothershipId)
    } else if (id in tierConfs) {
        return getMothershipId(tierConfs[id].planeId)
    }
    return undefined
}

const mothershipId = getMothershipId(id)
var mothership = mothershipId !== undefined ? new MothershipDBUpdater(mothershipId) : undefined

if (id in dataPlaneConfs) {
    if (destroy) {
        console.log(`Destruction of data-planes is not supported from launchpad, please delete it directly via pulumi CLI`)
        process.exit(1)
    }
    await setupDataPlane(dataPlaneConfs[id], preview, destroy)
    if (mothershipId !== undefined && mothership !== undefined) {
        console.log('updating mothership database...')
        await mothership.insertOrUpdateDataPlane(id, id => {
            if (id in customers) {
                return customers[id]
            }
            return undefined
        })
        process.once('exit', code => {
            if (mothership !== undefined) {
                mothership.exit().then(() => {
                    console.log(`closed mothership connection, exit code ${code}`)
                })
            }
        })
    }

} else if (id in tierConfs) {
    const tierConf = tierConfs[id];
    const dataplane = await setupDataPlane(dataPlaneConfs[tierConf.planeId], preview, false)
    if (destroy) {
        // For destroy we need to do a first pass of propagating protectResource to all the child
        // resources. So we run destroy as false.
        await setupTierWrapperFn(tierConf, dataplane, dataPlaneConfs[tierConf.planeId], preview, false, true)
    }
    // If destroy was set to true then both destroy and unprotect would be set to false and stack
    // destruction would continue.
    await setupTierWrapperFn(tierConf, dataplane, dataPlaneConfs[tierConf.planeId], preview, destroy, destroy)
    if (mothershipId !== undefined && mothership !== undefined) {
        console.log('updating mothership database...')
        await mothership.insertOrUpdateTier(id)
        process.once('exit', code => {
            if (mothership !== undefined) {
                mothership.exit().then(() => {
                    console.log(`closed mothership connection, exit code ${code}`)
                })
            }
        })
    }
} else if (id in mothershipConfs) {
    if (destroy) {
        console.log(`Destruction of mothership is not supported from launchpad, please delete it directly via pulumi CLI`)
        process.exit(1)
    }
    console.log("Updating mothership: ", id)
    await setupMothership(mothershipConfs[id], preview, destroy)
} else {
    console.log(`${id} is neither a tier, data plane or a control plane`)
    process.exit(1)
}

async function setupTierWrapperFn(tierConf: TierConf, dataplane: OutputMap, planeConf: DataPlaneConf, preview: boolean, destroy: boolean, unprotect: boolean) {
    const roleArn = dataplane[nameof<PlaneOutput>("roleArn")].value as string
    const dbOutput = dataplane[nameof<PlaneOutput>("db")].value as aurora.outputType
    const postgresDbOutput = dataplane[nameof<PlaneOutput>("postgresDb")].value as postgres.outputType
    const eksOutput = dataplane[nameof<PlaneOutput>("eks")].value as eks.outputType
    const redisOutput = dataplane[nameof<PlaneOutput>("redis")].value as redis.outputType
    const elasticacheOutput = dataplane[nameof<PlaneOutput>("elasticache")].value as elasticache.outputType
    const vpcOutput = dataplane[nameof<PlaneOutput>("vpc")].value as vpc.outputType
    const trainingDataOutput = dataplane[nameof<PlaneOutput>("trainingData")].value as connsink.outputType
    const offlineAggregateSourceFiles = dataplane[nameof<PlaneOutput>("offlineAggregateSourceFiles")].value as offlineAggregateSource.outputType
    const glueOutput = dataplane[nameof<PlaneOutput>("glue")].value as glueSource.outputType
    const telemetryOutput = dataplane[nameof<PlaneOutput>("telemetry")].value as telemetry.outputType
    const milvusOutput = dataplane[nameof<PlaneOutput>("milvus")].value as milvus.outputType
    const mskOutput = dataplane[nameof<PlaneOutput>("msk")].value as msk.outputType
    const nitrousOutput = dataplane[nameof<PlaneOutput>("nitrous")].value as nitrous.outputType

    // Create/update/delete the tier.
    const tierId = tierConf.tierId;
    console.log("Updating tier: ", tierId);
    if (unprotect) {
        tierConf.protectResources = false
    }

    // TODO(mohit): Validate that the nodeLabel specified in `PodConf` have at least one label match across labels
    // defined in all node groups.

    const mskConf: TierMskConf = {
        clusterArn: mskOutput.clusterArn,
        mskUsername: mskOutput.mskUsername,
        mskPassword: mskOutput.mskPassword,
        bootstrapBrokers: mskOutput.bootstrapBrokers,
        bootstrapBrokersIam: mskOutput.bootstrapBrokersIam,
        sgId: mskOutput.clusterSgId,
        s3ConnectPluginArn: mskOutput.s3ConnectPluginArn,
        s3ConnectPluginRev: mskOutput.s3ConnectPluginRevision,
        s3ConnectWorkerArn: mskOutput.s3ConnectWorkerArn,
        s3ConnectWorkerRev: mskOutput.s3ConnectWorkerRev,
    }

    const topics: kafkatopics.topicConf[] = [
        {
            name: `t_${tierId}_actionlog`,
            // TODO(mohit): Increase this period to 21 days to support few of the larger aggregates
            retention_ms: 1209600000  // 14 days retention
        },
        {
            name: `t_${tierId}_featurelog`,
            partitions: 10,
            retention_ms: 432000000  // 5 days retention
        },
        // configure profile topic to have "unlimited" retention
        {
            name: `t_${tierId}_profilelog`,
            retention_ms: -1
        },
        {
            name: `t_${tierId}_actionlog_json`,
            retention_ms: 432000000  // 5 days retention
        },
        { name: `t_${tierId}_aggr_offline_transform` },
        // configure stream log to which airbyte connectors will write stream data to
        {
            name: `t_${tierId}_streamlog`,
            partitions: 10,
            retention_ms: 432000000  // 5 days retention
        },
        {
            name: `t_${tierId}_hourly_usage_log`,
            retention_ms: 432000000  // 5 days retention
        }

    ];
    await setupTier({
        protect: tierConf.protectResources,

        tierId: Number(tierId),
        tierName: tierConf.tierName,
        planeId: Number(planeConf.planeId),

        topics: topics,
        mskConf: mskConf,

        connUserAccessKey: trainingDataOutput.userAccessKeyId,
        connUserSecret: trainingDataOutput.userSecretAccessKey,
        connBucketName: trainingDataOutput.bucketName,

        db: "db",
        dbEndpoint: dbOutput.host,
        dbUsername: "admin",
        dbPassword: planeConf.dbConf.password,

        postgresDbEndpoint: postgresDbOutput.host,
        postgresDbPort: postgresDbOutput.port,

        roleArn: roleArn,
        region: planeConf.region,

        kubeconfig: JSON.stringify(eksOutput.kubeconfig),
        namespace: `t-${tierId}`,

        redisEndpoint: redisOutput.clusterEndPoints[0],
        cachePrimaryEndpoint: elasticacheOutput.endpoint,

        ingressConf: tierConf.ingressConf,
        vpcPrivateSubnetIds: vpcOutput.privateSubnets,
        vpcPublicSubnetIds: vpcOutput.publicSubnets,

        clusterName: eksOutput.clusterName,
        nodeInstanceRoleArn: eksOutput.instanceRoleArn,

        glueSourceBucket: glueOutput.scriptSourceBucket,
        glueSourceScript: glueOutput.scriptPath,
        glueTrainingDataBucket: trainingDataOutput.bucketName,

        offlineAggregateSourceBucket: offlineAggregateSourceFiles.bucketName,
        offlineAggregateSourceFiles: offlineAggregateSourceFiles.sources,

        otelCollectorEndpoint: telemetryOutput.otelCollectorEndpoint,
        otelCollectorHttpEndpoint: telemetryOutput.otelCollectorHttpEndpoint,

        httpServerConf: tierConf.httpServerConf,
        queryServerConf: tierConf.queryServerConf,
        nitrousBinLogPartitions: nitrousOutput.binlogPartitions,

        countAggrConf: tierConf.countAggrConf,

        nodeInstanceRole: eksOutput.instanceRole,

        vpcId: vpcOutput.vpcId,
        connectedSecurityGroups: {
            "eks": eksOutput.clusterSg,
        },
        milvusEndpoint: milvusOutput.endpoint,
        sagemakerConf: tierConf.sagemakerConf,

        enableTrainingDatasetGenerationJobs: tierConf.enableTrainingDatasetGenerationJobs,
        enableOfflineAggregationJobs: tierConf.enableOfflineAggregationJobs,

        airbyteConf: tierConf.airbyteConf,
        plan: tierConf.plan,
        requestLimit: tierConf.requestLimit,

        enableCors: tierConf.enableCors,
    }, preview, destroy).catch(err => console.log(err))
}
