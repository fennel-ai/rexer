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
    primaryPrivateSubnet: "subnet-09927ea3f70675eff",
    secondaryPrivateSubnet: "subnet-0717483b092b91e73",
    primaryPublicSubnet: "subnet-0405bd564c9c0c456",
    secondaryPublicSubnet: "subnet-0f30e65832c38a357",
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
        id: 4,
        domain: "yext.com",
        name: "Yext",
    }
};
//================ Static data plane / tier configurations =====================

// map from tier id to plane id.
const tierConfs: Record<number, TierConf> = {
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
}

// map from plane id to its configuration.
const dataPlaneConfs: Record<number, DataPlaneConf> = {
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
                // Nitrous node group.
                {
                    name: "p-9-nitrous-ng-arm",
                    instanceTypes: ["c6gd.large"],
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
                // Nitrous backup node group.
                {
                    name: "p-9-nitrous-backup-ng-arm",
                    instanceTypes: ["c6gd.large"],
                    minSize: 1,
                    maxSize: 1,
                    amiType: DEFAULT_ARM_AMI_TYPE,
                    capacityType: ON_DEMAND_INSTANCE_TYPE,
                    labels: {
                        "node-group": "p-9-nitrous-backup-ng",
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
        nitrousConf: {
            replicas: 1,
            useAmd64: false,
            storageCapacityGB: 50,
            storageClass: "local",
            resourceConf: {
                cpu: {
                    request: "1200m",
                    limit: "2000m"
                },
                memory: {
                    request: "2Gi",
                    limit: "4Gi",
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

            // backup configurations
            backupConf: {
                nodeLabelsForBackup: {
                    "node-group": "p-9-nitrous-backup-ng",
                },
                backupFrequencyDuration: "60m",
                remoteCopiesToKeep: 2,
                resourceConf: {
                    cpu: {
                        request: "1200m",
                        limit: "2000m"
                    },
                    memory: {
                        request: "2Gi",
                        limit: "4Gi",
                    }
                },
                storageCapacityGB: 50,
            },
        },
        customer: customers[2],
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
            minCapacity: 1,
            maxCapacity: 8,
            password: "foundationdb",
            skipFinalSnapshot: false,
        },
        ingressConf: {
            useDedicatedMachines: false,
            usePublicSubnets: true,
            replicas: 2,
        },
        eksConf: {
            nodeGroups: [
                {
                    name: "m-12-common-ng-x86",
                    instanceTypes: ["t3.medium"],
                    minSize: 4,
                    maxSize: 6,
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
