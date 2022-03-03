import setupTier from "./tier";
import setupDataPlane from "./plane";
import * as vpc from "../vpc";

import * as process from "process";

// const tierId = process.env.TIER_ID!;

// setupTier(
//     {
//         tierId: parseInt(tierId),

//         bootstrapServer: process.env.KAFKA_SERVER_ADDRESS!,
//         topicNames: [`t_${tierId}_actionlog`, `t_${tierId}_featurelog`],
//         kafkaApiKey: process.env.KAFKA_USERNAME!,
//         kafkaApiSecret: process.env.KAFKA_PASSWORD!,

//         db: process.env.MYSQL_DATABASE_NAME!,
//         dbEndpoint: process.env.MYSQL_SERVER_ADDRESS!,
//         dbUsername: process.env.MYSQL_USERNAME!,
//         dbPassword: process.env.MYSQL_PASSWORD!,

//         roleArn: process.env.AWS_ROLE_ARN!,
//         region: process.env.AWS_REGION!,

//         kubeconfig: process.env.KUBECONFIG!,
//         namespace: `t-${tierId}`,

//         redisEndpoint: process.env.REDIS_SERVER_ADDRESS!,
//         cachePrimaryEndpoint: process.env.CACHE_PRIMARY!,
//         cacheReplicaEndpoint: process.env.CACHE_REPLICA!,

//         subnetIds: ["subnet-07b7f4dc20c5b9258", "subnet-0f81a1af4aee30667"],
//         loadBalancerScheme: "internal",
//     },
//     true,
// ).catch(err => console.log(err))

const planeId = 1;

const controlPlane: vpc.controlPlaneConfig = {
    region: "us-west-2",
    accountId: "030813887342",
    vpcId: "vpc-0d9942e83f94c049c",
    roleArn: "arn:aws:iam::030813887342:role/admin",
    routeTableId: "rtb-07afe7458db9c4479",
    cidrBlock: "172.31.0.0/16"
}

setupDataPlane(
    {
        planeId: Number(planeId),
        region: "ap-south-1",
        roleArn: "arn:aws:iam::136736114676:role/admin",
        vpcConf: {
            cidr: "10.101.0.0/16",
        },
        controlPlaneConf: controlPlane,
        dbConf: {
            minCapacity: 1,
            maxCapacity: 4,
            password: "password",
        }
    },
    false
)
