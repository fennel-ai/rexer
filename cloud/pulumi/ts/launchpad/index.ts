import setupTier from "./tier";
import setupDataPlane, { PlaneOutput } from "./plane";
import * as vpc from "../vpc";
import * as eks from "../eks";
import * as aurora from "../aurora";
import * as elasticache from "../elasticache";
import * as redis from "../redis";
import * as confluentenv from "../confluentenv";

import * as process from "process";
import * as assert from "assert";
import { nameof } from "../lib/util";

const controlPlane: vpc.controlPlaneConfig = {
    region: "us-west-2",
    accountId: "030813887342",
    vpcId: "vpc-0d9942e83f94c049c",
    roleArn: "arn:aws:iam::030813887342:role/admin",
    routeTableId: "rtb-07afe7458db9c4479",
    cidrBlock: "172.31.0.0/16"
}


//================== Data plane configuration variables ========================

const dataPlaneRegion = "us-west-2"
const dataPlaneRoleArn = "arn:aws:iam::030813887342:role/admin"
const dataPlaneCidr = "10.102.0.0/16"
const dbPassword = "foundationdb"
const dbMinCapacity = 4
const dbMaxCapacity = 16
const planeId = 2;
const tierId = 103;
const confluentUsername = process.env.CONFLUENT_CLOUD_USERNAME;
assert.ok(confluentUsername, "CONFLUENT_CLOUD_USERNAME must be set");
const confluentPassword = process.env.CONFLUENT_CLOUD_PASSWORD;
assert.ok(confluentPassword, "CONFLUENT_CLOUD_PASSWORD must be set");

//==============================================================================

const dataplane = await setupDataPlane({
    planeId: Number(planeId),
    region: dataPlaneRegion,
    roleArn: dataPlaneRoleArn,
    vpcConf: {
        cidr: dataPlaneCidr,
    },
    controlPlaneConf: controlPlane,
    dbConf: {
        password: dbPassword,
        minCapacity: dbMinCapacity,
        maxCapacity: dbMaxCapacity,
    },
    confluentConf: {
        username: confluentUsername,
        password: confluentPassword,
    },
})

const confluentOutput = dataplane[nameof<PlaneOutput>("confluent")].value as confluentenv.outputType
const dbOutput = dataplane[nameof<PlaneOutput>("db")].value as aurora.outputType
const eksOutput = dataplane[nameof<PlaneOutput>("eks")].value as eks.outputType
const redisOutput = dataplane[nameof<PlaneOutput>("redis")].value as redis.outputType
const elasticacheOutput = dataplane[nameof<PlaneOutput>("elasticache")].value as elasticache.outputType
const vpcOutput = dataplane[nameof<PlaneOutput>("vpc")].value as vpc.outputType

setupTier({
    tierId: Number(tierId),

    bootstrapServer: confluentOutput.bootstrapServer,
    topicNames: [`t_${tierId}_actionlog`, `t_${tierId}_featurelog`],
    kafkaApiKey: confluentOutput.apiKey,
    kafkaApiSecret: confluentOutput.apiSecret,

    db: "db",
    dbEndpoint: dbOutput.host,
    dbUsername: "admin",
    dbPassword: dbPassword,

    roleArn: dataPlaneRoleArn,
    region: dataPlaneRegion,

    kubeconfig: JSON.stringify(eksOutput.kubeconfig),
    namespace: `t-${tierId}`,

    redisEndpoint: redisOutput.clusterEndPoints[0],
    cachePrimaryEndpoint: elasticacheOutput.endpoint,
    subnetIds: vpcOutput.privateSubnets,
    loadBalancerScheme: "internal",
},
).catch(err => console.log(err))
