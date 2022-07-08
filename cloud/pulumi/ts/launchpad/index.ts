import setupTier, { TierConf } from "./tier";
import setupDataPlane, { PlaneConf, PlaneOutput } from "./plane";
import * as vpc from "../vpc";
import * as eks from "../eks";
import * as account from "../account";
import * as aurora from "../aurora";
import * as unleashDb from "../unleash-postgres";
import * as elasticache from "../elasticache";
import * as redis from "../redis";
import * as confluentenv from "../confluentenv";
import * as connsink from "../connectorsink";
import * as offlineAggregateSource from "../offline-aggregate-script-source";
import * as glueSource from "../glue-script-source";
import * as kafkatopics from "../kafkatopics";
import * as telemetry from "../telemetry";
import * as milvus from "../milvus";
import { nameof } from "../lib/util";

import * as process from "process";
import * as assert from "assert";

// NOTE: The AMI used should be an eks-worker AMI that can be searched
// on the AWS AMI catalog with one of the following prefixes:
// amazon-eks-node / amazon-eks-gpu-node / amazon-eks-arm64-node,
// depending on the type of machine provisioned.

const DEFAULT_NODE_TYPE = "t3.medium"
const DEFAULT_DESIRED_CAPACITY = 3
const DEFAULT_X86_AMI_TYPE = "AL2_x86_64"
const DEFAULT_ARM_AMI_TYPE = "AL2_ARM_64"

const controlPlane: vpc.controlPlaneConfig = {
    region: "us-west-2",
    accountId: "030813887342",
    vpcId: "vpc-0d9942e83f94c049c",
    roleArn: account.MASTER_ACCOUNT_ADMIN_ROLE_ARN,
    routeTableId: "rtb-07afe7458db9c4479",
    cidrBlock: "172.31.0.0/16"
}

//================ Static data plane / tier configurations =====================

const confluentUsername = process.env.CONFLUENT_CLOUD_USERNAME;
assert.ok(confluentUsername, "CONFLUENT_CLOUD_USERNAME must be set");
const confluentPassword = process.env.CONFLUENT_CLOUD_PASSWORD;
assert.ok(confluentPassword, "CONFLUENT_CLOUD_PASSWORD must be set");

// https://kubernetes-sigs.github.io/aws-load-balancer-controller/v2.2/guide/service/annotations/#resource-attributes
const PUBLIC_LB_SCHEME = "internet-facing";
const PRIVATE_LB_SCHEME = "internal";

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
                minReplicas: 8,
                maxReplicas: 10,
                nodeLabels: {
                    "node-group": "p-5-queryserver-ng"
                },
                resourceConf: {
                    cpu: {
                        request: "12000m",
                        limit: "15000m"
                    },
                    memory: {
                        request: "25G",
                        limit: "30G",
                    }
                },
                // set a threshold of 22G
                pprofHeapAllocThresholdMegaBytes: 22 << 10,
            }
        },
        sagemakerConf: {
            // this is the cheapest sagemaker instance type other than burstable instances (t3, t4g.. - but they are
            // autoscalable).
            instanceType: "ml.c5.large",
            // have multiple instances for fault tolerance
            instanceCount: 2,
        },
        ingressConf: {
            useDedicatedMachines: true,
        }
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
        planeId: 6,
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
                        limit: "2500m"
                    },
                    memory: {
                        request: "3G",
                        limit: "5G",
                    }
                }
            },
        },
    },
    110: {
        protectResources: false,
        planeId: 7,
        // use public subnets for ingress to allow traffic from outside the assigned VPC
        ingressConf: {
            usePublicSubnets: true,
        },
        httpServerConf: {
            podConf: {
                minReplicas: 1,
                maxReplicas: 1,
                resourceConf: {
                    cpu: {
                        request: "750m",
                        limit: "1500m"
                    },
                    memory: {
                        request: "1G",
                        limit: "3G",
                    }
                },
                nodeLabels: {
                    "node-group": "http-arm-ng"
                }
            }
        },
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
    // Gopuff demo tier
    113: {
        protectResources: true,
        planeId: 3,
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
        },
    },
    // Discord demo tier
    114: {
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
    },
}

// map from plane id to its configuration.
const planeConfs: Record<number, PlaneConf> = {
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
                // plane-level services like nitrous and milvus.
                {
                    name: "p-2-common-ng",
                    nodeType: "c6i.xlarge",
                    minSize: 3,
                    maxSize: 5,
                    amiType: DEFAULT_X86_AMI_TYPE,
                },
                // TODO: For nitrous, we may need to spin up ARM specific node group
            ],
        },
        milvusConf: {},
        // Run nitrous on the plane.
        nitrousConf: {
            replicas: 1,
            storageCapacityGB: 10,
            storageClass: "io1",
            blockCacheMB: 512,
            kvCacheMB: 1024,
            binlog: {},
        }
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
                    nodeType: "c7g.2xlarge",
                    minSize: 2,
                    // since we create demo tiers on top of this plane, allow scaling this node group to a larger
                    // number to accommodate more servers
                    maxSize: 10,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                },
                {
                    name: "p-3-common-ng-x86",
                    nodeType: "c6i.2xlarge",
                    // since we create demo tiers on top of this plane, allow scaling this node group to a larger
                    // number to accommodate more servers
                    //
                    // milvus requires minimum 3 nodes
                    minSize: 4,
                    maxSize: 10,
                    amiType: DEFAULT_X86_AMI_TYPE,
                },
            ],
        },
        milvusConf: {},
        // Run nitrous on the plane.
        nitrousConf: {
            replicas: 1,
            storageCapacityGB: 100,
            storageClass: "io1",
            blockCacheMB: 512,
            kvCacheMB: 1024,
            binlog: {},
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
            numShards: 50,
            nodeType: "db.r6g.large",
            numReplicasPerShard: 1,
        },
        eksConf: {
            nodeGroups: [
                // HTTP server node group
                {
                    name: "p-5-httpserver-ng-arm64",
                    nodeType: "t4g.medium",
                    // at least have 2 nodes for fault tolerance
                    minSize: 2,
                    maxSize: 5,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    labels: {
                        "node-group": "p-5-httpserver-ng"
                    }
                },
                // Countaggr server node group
                {
                    name: "p-5-countaggr-ng-arm64",
                    // TODO(mohit): Move to c7g once they are supported in ap-south-1
                    nodeType: "c6g.4xlarge",
                    minSize: 1,
                    maxSize: 1,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    labels: {
                        "node-group": "p-5-countaggr-ng"
                    }
                },
                // Query server node group
                {
                    name: "p-5-queryserver-ng-arm64",
                    // TODO(mohit): Move to c7g once they are supported in ap-south-1
                    nodeType: "c6g.4xlarge",
                    // at least have 4 nodes (previously this was 2, but our servers have been OOMing for which
                    // we will have 4 nodes up and running).
                    minSize: 4,
                    maxSize: 10,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    labels: {
                        "node-group": "p-5-queryserver-ng"
                    }
                },
                {
                    name: "p-5-common-ng-x86",
                    nodeType: "t3.medium",
                    // few pods still require X86 based machines and are not compatible with ARM64.
                    minSize: 2,
                    maxSize: 10,
                    amiType: DEFAULT_X86_AMI_TYPE,
                }
            ],
        },
        prometheusConf: {
            useAMP: false
        }
    },
    // Lokal's staging data plane
    6: {
        protectResources: true,

        accountConf: {
            existingAccount: {
                roleArn: account.MASTER_ACCOUNT_ADMIN_ROLE_ARN,
            }
        },

        planeId: 6,
        region: "ap-south-1",
        vpcConf: {
            cidr: "10.106.0.0/16"
        },
        dbConf: {
            minCapacity: 1,
            maxCapacity: 4,
            password: "password",
            skipFinalSnapshot: true,
        },
        eksConf: {
            nodeGroups: [
                {
                    name: "p-6-common-ng-x86",
                    nodeType: "t3.medium",
                    minSize: 1,
                    maxSize: 3,
                    amiType: DEFAULT_X86_AMI_TYPE,
                },
                {
                    name: "p-6-common-ng-arm64",
                    nodeType: "c6g.xlarge",
                    minSize: 1,
                    maxSize: 3,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                },
            ],
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
            nodeType: "cache.t4g.micro",
            numNodeGroups: 1,
            replicasPerNodeGroup: 0,
        },
        prometheusConf: {
            useAMP: false
        },
    },
    7: {
        protectResources: true,

        accountConf: {
            existingAccount: {
                roleArn: account.MASTER_ACCOUNT_ADMIN_ROLE_ARN,
            }
        },

        planeId: 7,
        region: "us-west-2",
        vpcConf: {
            cidr: "10.107.0.0/16"
        },
        dbConf: {
            minCapacity: 1,
            maxCapacity: 2,
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
            nodeType: "db.t4g.small",
            numReplicasPerShard: 0,
        },
        cacheConf: {
            nodeType: "cache.t4g.micro",
            numNodeGroups: 1,
            replicasPerNodeGroup: 0,
        },
        prometheusConf: {
            // TODO(mohit): Set this to true and false both and set it up
            useAMP: false
        },
        // Create two node groups. 1 with ARM backed instances and another with x86 backed instances
        eksConf: {
            nodeGroups: [
                {
                    name: "x86-ng2",
                    nodeType: "t3.medium",
                    minSize: 2,
                    maxSize: 2,
                    amiType: DEFAULT_X86_AMI_TYPE,
                },
                {
                    name: "http-arm-ng",
                    nodeType: "t4g.medium",
                    minSize: 1,
                    maxSize: 3,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    labels: {
                        "node-group": "http-arm-ng",
                    }
                },
                {
                    name: "x86-ng",
                    nodeType: "c6i.2xlarge",
                    minSize: 4,
                    maxSize: 5,
                    amiType: DEFAULT_X86_AMI_TYPE,
                },
            ],
        },
        // TODO(mohit): Add milvus and see how it works?
        milvusConf: {},
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
                    nodeType: "t3.medium",
                    minSize: 1,
                    maxSize: 3,
                    amiType: DEFAULT_X86_AMI_TYPE,
                },
                {
                    name: "p-9-common-ng-arm64",
                    nodeType: "t4g.medium",
                    minSize: 1,
                    maxSize: 3,
                    amiType: DEFAULT_ARM_AMI_TYPE,
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
if (id in planeConfs) {
    planeId = id
} else if (id in tierConfs) {
    tierId = id
    planeId = tierConfs[tierId].planeId
} else {
    console.log(`${id} is neither a tier nor a plane`)
    process.exit(1)
}

console.log("Updating plane: ", planeId)
const planeConf = planeConfs[planeId]
const dataplane = await setupDataPlane(planeConf, preview, destroy);

const roleArn = dataplane[nameof<PlaneOutput>("roleArn")].value as string
const confluentOutput = dataplane[nameof<PlaneOutput>("confluent")].value as confluentenv.outputType
const dbOutput = dataplane[nameof<PlaneOutput>("db")].value as aurora.outputType
const unleashDbOutput = dataplane[nameof<PlaneOutput>("unleashDb")].value as unleashDb.outputType
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
    ];
    setupTier({
        protect: tierConf.protectResources,

        tierId: Number(tierId),
        planeId: Number(planeId),

        bootstrapServer: confluentOutput.bootstrapServer,
        topics: topics,
        kafkaApiKey: confluentOutput.apiKey,
        kafkaApiSecret: confluentOutput.apiSecret,

        confUsername: confluentUsername,
        confPassword: confluentPassword,
        clusterId: confluentOutput.clusterId,
        environmentId: confluentOutput.environmentId,
        connUserAccessKey: trainingDataOutput.userAccessKeyId,
        connUserSecret: trainingDataOutput.userSecretAccessKey,
        connBucketName: trainingDataOutput.bucketName,

        db: "db",
        dbEndpoint: dbOutput.host,
        dbUsername: "admin",
        dbPassword: planeConf.dbConf.password,

        unleashDbEndpoint: unleashDbOutput.host,
        unleashDbPort: unleashDbOutput.port,

        roleArn: roleArn,
        region: planeConf.region,

        kubeconfig: JSON.stringify(eksOutput.kubeconfig),
        namespace: `t-${tierId}`,

        redisEndpoint: redisOutput.clusterEndPoints[0],
        cachePrimaryEndpoint: elasticacheOutput.endpoint,

        subnetIds: subnetIds,
        loadBalancerScheme: loadBalancerScheme,
        ingressUseDedicatedMachines: tierConf.ingressConf?.useDedicatedMachines,
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

        countAggrConf: tierConf.countAggrConf,

        nodeInstanceRole: eksOutput.instanceRole,

        vpcId: vpcOutput.vpcId,
        connectedSecurityGroups: {
            "eks": eksOutput.clusterSg,
        },
        milvusEndpoint: milvusOutput.endpoint,
        sagemakerConf: tierConf.sagemakerConf,
    }, preview, destroy).catch(err => console.log(err))
}
