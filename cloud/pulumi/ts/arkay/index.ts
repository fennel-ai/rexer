import setupTier from "./tier";

import * as process from "process";

const tierId = process.env.TIER_ID!;

setupTier(
    {
        tierId: parseInt(tierId),

        bootstrapServer: process.env.KAFKA_SERVER_ADDRESS!,
        topicNames: [`t_${tierId}_actionlog`, `t_${tierId}_featurelog`],
        kafkaApiKey: process.env.KAFKA_USERNAME!,
        kafkaApiSecret: process.env.KAFKA_PASSWORD!,

        db: process.env.MYSQL_DATABASE_NAME!,
        dbEndpoint: process.env.MYSQL_SERVER_ADDRESS!,
        dbUsername: process.env.MYSQL_USERNAME!,
        dbPassword: process.env.MYSQL_PASSWORD!,

        roleArn: process.env.AWS_ROLE_ARN!,
        region: process.env.AWS_REGION!,

        kubeconfig: process.env.KUBECONFIG!,
        namespace: `t-${tierId}`,

        redisEndpoint: process.env.REDIS_SERVER_ADDRESS!,
        cachePrimaryEndpoint: process.env.CACHE_PRIMARY!,
        cacheReplicaEndpoint: process.env.CACHE_REPLICA!,

        subnetIds: ["subnet-05f8b4e79c24389a5", "subnet-0cc2d645a7c062809"],
        loadBalancerScheme: "internet-facing",
    },
    true,
).catch(err => console.log(err))