import setupTier, { TierConf } from "./tier";
import setupDataPlane, { PlaneConf, PlaneOutput } from "./plane";
import * as vpc from "../vpc";
import * as eks from "../eks";
import * as aurora from "../aurora";
import * as elasticache from "../elasticache";
import * as redis from "../redis";
import * as confluentenv from "../confluentenv";
import * as connsink from "../connectorsink";
import * as offlineAggregateSource from "../offline-aggregate-script-source";
import * as glueSource from "../glue-script-source";
import * as kafkatopics from "../kafkatopics";
import { nameof } from "../lib/util";

import * as process from "process";
import * as assert from "assert";

const controlPlane: vpc.controlPlaneConfig = {
    region: "us-west-2",
    accountId: "030813887342",
    vpcId: "vpc-0d9942e83f94c049c",
    roleArn: "arn:aws:iam::030813887342:role/admin",
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
    // Aditya's new dev tier.
    104: {
        planeId: 3,
        httpServerConf: {
            podConf: {
                replicas: 1,
                // each http-server should be in different nodes from each other
                enforceReplicaIsolation: false,
            }
        },
    },
    // Fennel staging tier using Fennel's staging data plane.
    106: {
        planeId: 3,
        httpServerConf: {
            podConf: {
                replicas: 1,
                // each http-server should be in different nodes from each other
                enforceReplicaIsolation: false,
            }
        },
        apiServerConf: {
            podConf: {
                replicas: 1,
                enforceReplicaIsolation: false,
            },
            // This will be replaced with the actual storageclass
            // of the type io1.
            storageclass: "io1",
        },
    },
    // Lokal prod tier on their prod data plane.
    107: {
        planeId: 5,
        httpServerConf: {
            podConf: {
                replicas: 2,
                // each http-server should be in different nodes from each other
                enforceReplicaIsolation: true,
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
        queryServerConf: {
            podConf: {
                replicas: 4,
                enforceReplicaIsolation: true,
                nodeLabels: {
                    "node-group": "p-5-queryserver-ng"
                }
            }
        }
    },
    // Convoy staging tier using Fennel's staging data plane.
    108: {
        planeId: 3,
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
            loadBalancerScheme: PUBLIC_LB_SCHEME,
        },
        httpServerConf: {
            podConf: {
                replicas: 1,
                // each http-server should be in different nodes from each other
                enforceReplicaIsolation: false,
            },
        },
    },
    // Lokal's staging tier
    111: {
        planeId: 4,
        // use public subnets for ingress to allow traffic from outside the assigned vpc
        ingressConf: {
            usePublicSubnets: true,
            loadBalancerScheme: PUBLIC_LB_SCHEME,
        }
    }
}

// map from plane id to its configuration.
const planeConfs: Record<number, PlaneConf> = {
    // this is used for test resources
    2: {
        planeId: 2,
        region: "us-west-2",
        roleArn: "arn:aws:iam::030813887342:role/admin",
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
            nodeType: "db.t4g.medium",
        },
        prometheusConf: {
            useAMP: true
        }
    },
    // Fennel's staging data plane to run dev tiers
    3: {
        planeId: 3,
        region: "us-west-2",
        roleArn: "arn:aws:iam::030813887342:role/admin",
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
            numShards: 2,
            nodeType: "db.t4g.medium",
        },
        prometheusConf: {
            useAMP: true
        },
        // increase the desired capacity and scale up to occupy more pods
        //
        // https://github.com/awslabs/amazon-eks-ami/blob/master/files/eni-max-pods.txt
        eksConf: {
            nodeGroups: [
                {
                    name: "p-3-common-ng",
                    nodeType: "c6i.2xlarge",
                    desiredCapacity: 4,
                },
            ],
        },
        milvusConf: {},
    },
    // Lokal's dev tier data plane.
    4: {
        planeId: 4,
        region: "ap-south-1",
        roleArn: "arn:aws:iam::030813887342:role/admin",
        vpcConf: {
            cidr: "10.104.0.0/16"
        },
        dbConf: {
            minCapacity: 1,
            maxCapacity: 64,
            password: "password",
            skipFinalSnapshot: false,
        },
        confluentConf: {
            username: confluentUsername,
            password: confluentPassword
        },
        cacheConf: {
            nodeType: "cache.t4g.medium",
            numNodeGroups: 2,
            replicasPerNodeGroup: 1,
        },
        controlPlaneConf: controlPlane,
        redisConf: {
            numShards: 4,
            nodeType: "db.t4g.medium",
            numReplicasPerShard: 1,
        },
        prometheusConf: {
            useAMP: false
        },
    },
    // Lokal's prod tier data plane
    5: {
        planeId: 5,
        region: "ap-south-1",
        roleArn: "arn:aws:iam::030813887342:role/admin",
        vpcConf: {
            cidr: "10.105.0.0/16"
        },
        dbConf: {
            minCapacity: 8,
            maxCapacity: 64,
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
            numShards: 45,
            nodeType: "db.r6g.large",
            numReplicasPerShard: 1,
        },
        eksConf: {
            nodeGroups: [
                // HTTP server node group
                {
                    name: "p-5-httpserver-ng",
                    nodeType: "c6i.4xlarge",
                    desiredCapacity: 2,
                    labels: {
                        "node-group": "p-5-httpserver-ng"
                    }
                },
                // Countaggr server node group
                {
                    name: "p-5-countaggr-ng",
                    nodeType: "c6i.8xlarge",
                    desiredCapacity: 1,
                    labels: {
                        "node-group": "p-5-countaggr-ng"
                    }
                },
                // Query server node group
                {
                    name: "p-5-queryserver-ng",
                    nodeType: "c6i.4xlarge",
                    desiredCapacity: 4,
                    labels: {
                        "node-group": "p-5-queryserver-ng"
                    }
                }
            ],
        },
        prometheusConf: {
            useAMP: false
        }
    },
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

const confluentOutput = dataplane[nameof<PlaneOutput>("confluent")].value as confluentenv.outputType
const dbOutput = dataplane[nameof<PlaneOutput>("db")].value as aurora.outputType
const eksOutput = dataplane[nameof<PlaneOutput>("eks")].value as eks.outputType
const redisOutput = dataplane[nameof<PlaneOutput>("redis")].value as redis.outputType
const elasticacheOutput = dataplane[nameof<PlaneOutput>("elasticache")].value as elasticache.outputType
const vpcOutput = dataplane[nameof<PlaneOutput>("vpc")].value as vpc.outputType
const trainingDataOutput = dataplane[nameof<PlaneOutput>("trainingData")].value as connsink.outputType
const offlineAggregateSourceFiles = dataplane[nameof<PlaneOutput>("offlineAggregateSourceFiles")].value as offlineAggregateSource.outputType
const glueOutput = dataplane[nameof<PlaneOutput>("glue")].value as glueSource.outputType

// Create/update/delete the tier.
if (tierId !== 0) {
    console.log("Updating tier: ", tierId);
    const tierConf = tierConfs[tierId]
    // by default use private subnets
    let subnetIds;
    const usePublicSubnets = tierConf.ingressConf?.usePublicSubnets || false;
    if (usePublicSubnets) {
        subnetIds = vpcOutput.publicSubnets;
    } else {
        subnetIds = vpcOutput.privateSubnets;
    }
    if (tierConf.apiServerConf?.storageclass !== undefined) {
        tierConf.apiServerConf.storageclass =
            eksOutput.storageclasses[tierConf.apiServerConf.storageclass]
    }

    // TODO(mohit): Validate that the nodeLabel specified in `PodConf` have at least one label match across labels
    // defined in all node groups.

    const topics: kafkatopics.topicConf[] = [
        { name: `t_${tierId}_actionlog` },
        { name: `t_${tierId}_featurelog`, partitions: 10 },
        { name: `t_${tierId}_profilelog` },
        { name: `t_${tierId}_actionlog_json` },
        { name: `t_${tierId}_aggr_delta` },
        { name: `t_${tierId}_aggr_offline_transform` },
    ];
    setupTier({
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

        roleArn: planeConf.roleArn,
        region: planeConf.region,

        kubeconfig: JSON.stringify(eksOutput.kubeconfig),
        namespace: `t-${tierId}`,

        redisEndpoint: redisOutput.clusterEndPoints[0],
        cachePrimaryEndpoint: elasticacheOutput.endpoint,
        subnetIds: subnetIds,
        loadBalancerScheme: tierConf.ingressConf?.loadBalancerScheme || PRIVATE_LB_SCHEME,

        glueSourceBucket: glueOutput.scriptSourceBucket,
        glueSourceScript: glueOutput.scriptPath,
        glueTrainingDataBucket: trainingDataOutput.bucketName,

        offlineAggregateSourceBucket: offlineAggregateSourceFiles.bucketName,
        offlineAggregateSourceFiles: offlineAggregateSourceFiles.sources,

        httpServerConf: tierConf.httpServerConf,

        queryServerConf: tierConf.queryServerConf,

        countAggrConf: tierConf.countAggrConf,

        apiServerConf: tierConf.apiServerConf,

        nodeInstanceRole: eksOutput.instanceRole,

        vpcId: vpcOutput.vpcId,
        connectedSecurityGroups: {
            "eks": eksOutput.clusterSg,
        },
    }, preview, destroy).catch(err => console.log(err))
}
