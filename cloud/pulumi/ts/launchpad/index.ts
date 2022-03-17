import setupTier from "./tier";
import setupDataPlane, { PlaneConf, PlaneOutput } from "./plane";
import * as vpc from "../vpc";
import * as eks from "../eks";
import * as aurora from "../aurora";
import * as elasticache from "../elasticache";
import * as redis from "../redis";
import * as confluentenv from "../confluentenv";
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

// map from tier id to plane id.
const tierConfs: Record<number, number> = {
    // Trell-hosted tier.
    102: 1,
    // Aditya's new dev tier.
    104: 3,
    // Lokal prod tier.
    105: 4,
}

// map from plane id to its configuration.
const planeConfs: Record<number, PlaneConf> = {
    1: {
        planeId: 1,
        region: "ap-south-1",
        roleArn: "arn:aws:iam::136736114676:role/admin",
        vpcConf: {
            cidr: "10.101.0.0/16"
        },
        dbConf: {
            minCapacity: 1,
            maxCapacity: 4,
            password: "password"
        },
        confluentConf: {
            username: confluentUsername,
            password: confluentPassword
        },
        controlPlaneConf: controlPlane,
        prometheusConf: {
            useAMP: false
        }
    },
    // this is used for test resources
    2: {
        planeId: 2,
        region: "us-west-2",
        roleArn: "arn:aws:iam::030813887342:role/admin",
        vpcConf: {
            cidr: "10.102.0.0/16"
        },
        dbConf: {
            minCapacity: 4,
            maxCapacity: 4,
            password: "foundationdb"
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
    3: {
        planeId: 3,
        region: "us-west-2",
        roleArn: "arn:aws:iam::030813887342:role/admin",
        vpcConf: {
            cidr: "10.103.0.0/16"
        },
        dbConf: {
            minCapacity: 4,
            maxCapacity: 16,
            password: "foundationdb"
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
        }
    },
    4: {
        planeId: 4,
        region: "ap-south-1",
        roleArn: "arn:aws:iam::030813887342:role/admin",
        vpcConf: {
            cidr: "10.104.0.0/16"
        },
        dbConf: {
            minCapacity: 1,
            maxCapacity: 4,
            password: "password"
        },
        confluentConf: {
            username: confluentUsername,
            password: confluentPassword
        },
        controlPlaneConf: controlPlane,
        prometheusConf: {
            useAMP: false
        }
    },
}

//==============================================================================

var tierId = 0;
var planeId = 0;

const id = Number.parseInt(process.argv[process.argv.length - 1])
if (id in planeConfs) {
    planeId = id
} else if (id in tierConfs) {
    tierId = id
    planeId = tierConfs[tierId]
} else {
    console.log(`${id} is neither a tier nor a plane`)
    process.exit(1)
}

console.log("Updating plane: ", planeId)
const planeConf = planeConfs[planeId]
const dataplane = await setupDataPlane(planeConf);

const confluentOutput = dataplane[nameof<PlaneOutput>("confluent")].value as confluentenv.outputType
const dbOutput = dataplane[nameof<PlaneOutput>("db")].value as aurora.outputType
const eksOutput = dataplane[nameof<PlaneOutput>("eks")].value as eks.outputType
const redisOutput = dataplane[nameof<PlaneOutput>("redis")].value as redis.outputType
const elasticacheOutput = dataplane[nameof<PlaneOutput>("elasticache")].value as elasticache.outputType
const vpcOutput = dataplane[nameof<PlaneOutput>("vpc")].value as vpc.outputType

// Create/update/delete the tier.
if (tierId !== 0) {
    console.log("Updating tier: ", tierId);
    setupTier({
        tierId: Number(tierId),

        bootstrapServer: confluentOutput.bootstrapServer,
        topicNames: [`t_${tierId}_actionlog`, `t_${tierId}_featurelog`, `t_${tierId}_profilelog`],
        kafkaApiKey: confluentOutput.apiKey,
        kafkaApiSecret: confluentOutput.apiSecret,

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
        subnetIds: vpcOutput.privateSubnets,
        loadBalancerScheme: "internal",
    }, false).catch(err => console.log(err))
}
