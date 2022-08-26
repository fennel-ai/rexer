import setupTier, { TierConf } from "./tier";
import setupDataPlane, { DataPlaneConf, PlaneOutput } from "./data_plane";
import setupMothership, { MothershipConf } from "./mothership";
import * as vpc from "../vpc";
import * as eks from "../eks";
import * as account from "../account";
import * as aurora from "../aurora";
import * as postgres from "../postgres";
import * as elasticache from "../elasticache";
import * as redis from "../redis";
import * as confluentenv from "../confluentenv";
import * as connsink from "../connectorsink";
import * as offlineAggregateSource from "../offline-aggregate-script-source";
import * as glueSource from "../glue-script-source";
import * as kafkatopics from "../kafkatopics";
import * as telemetry from "../telemetry";
import * as milvus from "../milvus";
import { nameof, PUBLIC_LB_SCHEME, PRIVATE_LB_SCHEME } from "../lib/util";

import * as process from "process";
import * as assert from "assert";
import { DEFAULT_ARM_AMI_TYPE, DEFAULT_X86_AMI_TYPE, ON_DEMAND_INSTANCE_TYPE, SPOT_INSTANCE_TYPE } from "../eks";
import { OutputMap } from "@pulumi/pulumi/automation";

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

//================ Static data plane / tier configurations =====================

const confluentUsername = process.env.CONFLUENT_CLOUD_USERNAME;
assert.ok(confluentUsername, "CONFLUENT_CLOUD_USERNAME must be set");
const confluentPassword = process.env.CONFLUENT_CLOUD_PASSWORD;
assert.ok(confluentPassword, "CONFLUENT_CLOUD_PASSWORD must be set");

// map from tier id to plane id.
const tierConfs: Record<number, TierConf> = {
    // Fennel staging tier using Fennel's staging data plane.
    106: {
        protectResources: true,
        planeId: 3,
        httpServerConf: {
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
            }
        },
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
        }
    },
    // Lokal prod tier on their prod data plane.
    107: {
        protectResources: true,
        planeId: 5,
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
                }
            }
        },
        // TODO(mohit): Currently the requests are configured such that each replica is going to be scheduled
        // in different node in the node group, ideally we should try to reduce the `request` and let the scheduler
        // place the pods across the nodes based on utilization and `limit`
        queryServerConf: {
            podConf: {
                minReplicas: 4,
                maxReplicas: 10,
                nodeLabels: {
                    "node-group": "p-5-queryserver-ng"
                },
                resourceConf: {
                    cpu: {
                        request: "28000m",
                        limit: "31000m"
                    },
                    memory: {
                        request: "58G",
                        limit: "63G",
                    }
                },
                // set a threshold of 22G
                pprofHeapAllocThresholdMegaBytes: 50 << 10,
            }
        },
        sagemakerConf: {
            // this is the cheapest sagemaker instance type other than burstable instances (t3, t4g.. - but they are
            // not autoscalable).
            instanceType: "ml.c5.large",
            // have multiple instances for fault tolerance
            instanceCount: 3,
        },
        ingressConf: {
            useDedicatedMachines: true,
            replicas: 4,
        },
        enableNitrous: true,
    },
    // Convoy staging tier using Fennel's staging data plane.
    108: {
        protectResources: true,
        planeId: 3,
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
        },
        httpServerConf: {
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
                }
            },
        },
    },
    // Lokal's staging tier
    109: {
        protectResources: true,
        planeId: 5,
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
        },
        httpServerConf: {
            podConf: {
                minReplicas: 1,
                maxReplicas: 3,
                resourceConf: {
                    cpu: {
                        request: "750m",
                        limit: "1500m"
                    },
                    memory: {
                        request: "2G",
                        limit: "3G",
                    }
                }
            },
        },
        enableNitrous: true,
    },
    // Discord demo tier
    111: {
        protectResources: true,
        planeId: 3,
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
        },
        httpServerConf: {
            podConf: {
                minReplicas: 1,
                maxReplicas: 3,
                resourceConf: {
                    cpu: {
                        request: "2000m",
                        limit: "6000m"
                    },
                    memory: {
                        request: "6G",
                        limit: "8G",
                    }
                }
            },
        },
        enableNitrous: true,
    },
    // Convoy prod tier
    112: {
        protectResources: true,
        planeId: 9,
        // TODO(mohit): set service configurations
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
        },
    },
    // Yext demo tier
    115: {
        protectResources: true,
        planeId: 3,
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
        },
        // set larger requests for the http + query server
        httpServerConf: {
            podConf: {
                minReplicas: 1,
                maxReplicas: 4,
                resourceConf: {
                    cpu: {
                        request: "2250m",
                        limit: "2500m"
                    },
                    memory: {
                        request: "6G",
                        limit: "7G",
                    }
                }
            },
        },
        // enable airbyte
        airbyteConf: {
            publicServer: false,
        }
    },
    // 3 Demo tiers asked by Nikhil as of 08/09/2022
    116: {
        protectResources: true,
        planeId: 3,
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
        },
    },
    117: {
        protectResources: true,
        planeId: 3,
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
        }
    },
    118: {
        protectResources: true,
        planeId: 3,
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
        }
    }
}

// map from plane id to its configuration.
const dataPlaneConfs: Record<number, DataPlaneConf> = {
    // this is used for test resources
    2: {
        protectResources: true,

        accountConf: {
            existingAccount: {
                roleArn: account.MASTER_ACCOUNT_ADMIN_ROLE_ARN,
            }
        },

        planeId: 2,
        region: "us-west-2",
        vpcConf: {
            cidr: "10.102.0.0/16"
        },
        dbConf: {
            minCapacity: 8,
            maxCapacity: 8,
            password: "foundationdb",
            skipFinalSnapshot: true,
        },
        confluentConf: {
            username: confluentUsername,
            password: confluentPassword
        },
        controlPlaneConf: controlPlane,
        redisConf: {
            numShards: 1,
            nodeType: "db.t4g.small",
            numReplicasPerShard: 0,
        },
        cacheConf: {
            numNodeGroups: 1,
            nodeType: "cache.t4g.micro",
            replicasPerNodeGroup: 0,
        },
        prometheusConf: {
            useAMP: true
        },
        eksConf: {
            nodeGroups: [
                // Plane 2 does not run any tier-specific services, but needs to run
                // plane-level services like nitrous etc.
                {
                    name: "p-2-common-ng",
                    instanceTypes: ["t3.medium"],
                    minSize: 1,
                    maxSize: 3,
                    amiType: DEFAULT_X86_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                // TODO: For nitrous, we may need to spin up ARM specific node group
            ],
        },
        // set up MSK cluster for integration tests
        mskConf: {
            // compute cost = 0.0456 ($/hr) x 2 (#brokers) x 720 = $65.6
            brokerType: "kafka.t3.small",
            // this will place 1 broker node in each of the AZs
            numberOfBrokerNodes: 2,
            // storage cost = 0.10 ($/GB-month) x 64 = 6.4$
            storageVolumeSizeGiB: 64,
        },
    },
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
        confluentConf: {
            username: confluentUsername,
            password: confluentPassword
        },
        controlPlaneConf: controlPlane,
        redisConf: {
            numShards: 4,
            nodeType: "db.t4g.medium",
            numReplicasPerShard: 0,
        },
        prometheusConf: {
            useAMP: true
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
                    name: "p-3-common-ng-arm64",
                    instanceTypes: ["c7g.2xlarge"],
                    minSize: 2,
                    // since we create demo tiers on top of this plane, allow scaling this node group to a larger
                    // number to accommodate more servers
                    maxSize: 10,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                {
                    name: "p-3-common-ng-x86",
                    instanceTypes: ["c6i.2xlarge"],
                    // since we create demo tiers on top of this plane, allow scaling this node group to a larger
                    // number to accommodate more servers
                    //
                    // milvus requires minimum 3 nodes
                    minSize: 4,
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
            ],
        },
        milvusConf: {},
        modelMonitoringConf: {},
        // Run nitrous on the plane.
        nitrousConf: {
            replicas: 1,
            storageCapacityGB: 100,
            storageClass: "local",
            blockCacheMB: 512,
            kvCacheMB: 1024,
            binlog: {
                partitions: 10,
            },
            mskBinlog: {
                partitions: 10,
                // since we have created 2 broker nodes, RF has to be smaller than that
                replicationFactor: 1,
            },
            nodeLabels: {
                "node-group": "p-3-nitrous-ng",
            }
        },

        // set up MSK cluster
        mskConf: {
            brokerType: "kafka.m5.large",
            // this will place 1 broker node in each of the AZs
            numberOfBrokerNodes: 2,
            // consider expanding this in the future if each broker needs more storage capacity
            storageVolumeSizeGiB: 128,
        }
    },
    // Lokal's prod tier data plane
    5: {
        protectResources: true,

        accountConf: {
            existingAccount: {
                roleArn: account.MASTER_ACCOUNT_ADMIN_ROLE_ARN,
            }
        },

        planeId: 5,
        region: "ap-south-1",
        vpcConf: {
            cidr: "10.105.0.0/16"
        },
        dbConf: {
            minCapacity: 2,
            maxCapacity: 16,
            password: "password",
            skipFinalSnapshot: false,
        },
        confluentConf: {
            username: confluentUsername,
            password: confluentPassword
        },
        cacheConf: {
            nodeType: "cache.t4g.medium",
            numNodeGroups: 4,
            replicasPerNodeGroup: 1,
        },
        controlPlaneConf: controlPlane,
        redisConf: {
            numShards: 46,
            nodeType: "db.r6g.large",
            numReplicasPerShard: 1,
        },
        eksConf: {
            nodeGroups: [
                // TODO(mohit): Consider naming in a consistent way.. long names will hit character limits
                //
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
                    instanceTypes: ["c6g.8xlarge"],
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
                //
                // each of them have the same expansion priority - cluster autoscaler will pick one randomly and
                // try to scale them up. If any of them fails (due to lack of capacity), it will try another
                // node group immediately. It is okay for on-demand nodegroup to be scaled up, since
                // it will later be rescheduled to spot nodegroups using rescheduler.
                // ideally, on-demand nodegroup should have lower priority, but `priority` expander currently
                // does not take lower priority nodegroups into consideration for scaling.
                //
                // we set the maxSize of on-demand nodegroup as 10 - this is required because it is possible that
                // spot instances are not available at all, requiring all the instances to be scheduled on on-demand
                // instances - https://github.com/kubernetes/autoscaler/issues/4992
                //
                // TODO(mohit): Consider changing query server requests to <16cpu and <32G and provision 4xlarge node
                // groups
                {
                    name: "p-5-queryserver-ng-arm64",
                    // TODO(mohit): Move to c7g once they are supported in ap-south-1
                    instanceTypes: ["c6g.8xlarge"],
                    minSize: 1,
                    maxSize: 10,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    labels: {
                        "node-group": "p-5-queryserver-ng",
                        "rescheduler-label": "on-demand",
                    },
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 10,
                },
                {
                    name: "p-5-query-ng-32c-64G-arm-spot",
                    // TODO(mohit): Move to c7g once they are supported in ap-south-1
                    //
                    // TODO(mohit): Consider using NVMe SSD backed instances as well - these should be okay for
                    // query servers which are "stateless" anyways. However we do run few binaries which are stateful
                    // and should not be scheduled on these nodes
                    instanceTypes: ["c6g.8xlarge", "c6gn.8xlarge"],
                    minSize: 2,
                    maxSize: 10,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    labels: {
                        "node-group": "p-5-queryserver-ng",
                        "rescheduler-label": "spot",
                    },
                    capacityType: SPOT_INSTANCE_TYPE,
                    expansionPriority: 10,
                },
                {
                    name: "p-5-query-ng-32c-64G-amd-spot",
                    // TODO(mohit): Consider using NVMe SSD backed instances as well - these should be okay for
                    // query servers which are "stateless" anyways. However we do run few binaries which are stateful
                    // and should not be scheduled on these nodes
                    instanceTypes: ["c6i.8xlarge", "c6a.8xlarge"],
                    minSize: 1,
                    maxSize: 10,
                    amiType: DEFAULT_X86_AMI_TYPE,
                    labels: {
                        "node-group": "p-5-queryserver-ng",
                        "rescheduler-label": "spot",
                    },
                    capacityType: SPOT_INSTANCE_TYPE,
                    expansionPriority: 10,
                },
                // Nitrous node group.
                {
                    name: "p-5-nitrous-ng-arm",
                    instanceTypes: ["c6gd.8xlarge"],
                    minSize: 1,
                    maxSize: 1,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    labels: {
                        "node-group": "p-5-nitrous-ng",
                        "aws.amazon.com/eks-local-ssd": "true",
                    },
                    expansionPriority: 1,
                },
                {
                    name: "p-5-common-ng-x86",
                    instanceTypes: ["t3.medium"],
                    // few pods still require X86 based machines and are not compatible with ARM64.
                    minSize: 2,
                    maxSize: 10,
                    amiType: DEFAULT_X86_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                }
            ],
            spotReschedulerConf: {
                spotNodeLabel: "rescheduler-label=spot",
                onDemandNodeLabel: "rescheduler-label=on-demand",
            }
        },
        prometheusConf: {
            useAMP: false
        },
        // Run nitrous on the plane.
        nitrousConf: {
            replicas: 1,
            useAmd64: false,
            storageCapacityGB: 1700,
            storageClass: "local",
            blockCacheMB: 1024 * 8,
            kvCacheMB: 1024 * 75,
            resourceConf: {
                cpu: {
                    request: "30000m",
                    limit: "32000m"
                },
                memory: {
                    request: "60G",
                    limit: "64G",
                }
            },
            binlog: {
                partitions: 32,
                retention_ms: 30 * 24 * 60 * 60 * 1000 /* 30 days */,
                partition_retention_bytes: -1,
            },
            nodeLabels: {
                "node-group": "p-5-nitrous-ng",
            }
        }
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
                    maxSize: 3,
                    amiType: DEFAULT_X86_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    expansionPriority: 1,
                },
                {
                    name: "p-9-common-ng-arm64",
                    instanceTypes: ["t4g.medium"],
                    minSize: 1,
                    maxSize: 3,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
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
        confluentConf: {
            username: confluentUsername,
            password: confluentPassword
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
            useAMP: true
        },
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
            nodeGroups: [{
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
            }
        },

    }
}

//==============================================================================

var tierId = 0;
var planeId = 0;

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
    } else {
        console.log(`${action} is not a supported action`)
        process.exit(1)
    }
}

const id = Number.parseInt(process.argv[process.argv.length - 1])
if (id in dataPlaneConfs) {
    planeId = id
    console.log("Updating data plane: ", planeId)
    await setupDataPlane(dataPlaneConfs[planeId], preview, destroy)
} else if (id in tierConfs) {
    tierId = id
    planeId = tierConfs[tierId].planeId
    console.log("Updating data plane: ", planeId)
    const dataplane = await setupDataPlane(dataPlaneConfs[planeId], preview, destroy)
    setupTierWrapperFn(tierId, dataplane, dataPlaneConfs[planeId])
} else if (id in mothershipConfs) {
    planeId = id
    console.log("Updating mothership: ", planeId)
    await setupMothership(mothershipConfs[planeId], preview, destroy)
} else {
    console.log(`${id} is neither a tier, data plane or a control plane`)
    process.exit(1)
}

function setupTierWrapperFn(tierId: number, dataplane: OutputMap, planeConf: DataPlaneConf) {
    const roleArn = dataplane[nameof<PlaneOutput>("roleArn")].value as string
    const confluentOutput = dataplane[nameof<PlaneOutput>("confluent")].value as confluentenv.outputType
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

    // Create/update/delete the tier.
    if (tierId !== 0) {
        console.log("Updating tier: ", tierId);
        const tierConf = tierConfs[tierId]
        // by default use private subnets
        let subnetIds;
        let loadBalancerScheme;
        const usePublicSubnets = tierConf.ingressConf !== undefined ? tierConf.ingressConf.usePublicSubnets || false : false;
        if (usePublicSubnets) {
            subnetIds = vpcOutput.publicSubnets;
            loadBalancerScheme = PUBLIC_LB_SCHEME;
        } else {
            subnetIds = vpcOutput.privateSubnets;
            loadBalancerScheme = PRIVATE_LB_SCHEME;
        }

        // TODO(mohit): Validate that the nodeLabel specified in `PodConf` have at least one label match across labels
        // defined in all node groups.

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
        setupTier({
            protect: tierConf.protectResources,

            tierId: Number(tierId),
            planeId: Number(planeId),

            bootstrapServer: confluentOutput.bootstrapServer,
            topics: topics,
            kafkaApiKey: confluentOutput.apiKey,
            kafkaApiSecret: confluentOutput.apiSecret,

            confUsername: confluentUsername!,
            confPassword: confluentPassword!,
            clusterId: confluentOutput.clusterId,
            environmentId: confluentOutput.environmentId,
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

            subnetIds: subnetIds,
            loadBalancerScheme: loadBalancerScheme,
            ingressUseDedicatedMachines: tierConf.ingressConf?.useDedicatedMachines,
            ingressReplicas: tierConf.ingressConf?.replicas,
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
            enableNitrous: tierConf.enableNitrous,

            countAggrConf: tierConf.countAggrConf,

            nodeInstanceRole: eksOutput.instanceRole,

            vpcId: vpcOutput.vpcId,
            connectedSecurityGroups: {
                "eks": eksOutput.clusterSg,
            },
            milvusEndpoint: milvusOutput.endpoint,
            sagemakerConf: tierConf.sagemakerConf,

            airbyteConf: tierConf.airbyteConf,
        }, preview, destroy).catch(err => console.log(err))
    }
}