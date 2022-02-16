import * as confluent from "@pulumi/confluent";
import * as kafka from "@pulumi/kafka";
import * as pulumi from "@pulumi/pulumi";

type cluster = {
    environmentId: string,
    id: string,
    bootstrapServers: string,
}

export type input = {
    username: string,
    password: pulumi.Output<string>
    topicNames: [string],
    kafkaCluster: cluster
}

const parseConfig = (): input => {
    const config = new pulumi.Config();
    return {
        username: config.require("username"),
        password: config.requireSecret("password"),
        topicNames: config.requireObject("topicNames"),
        kafkaCluster: config.requireObject("cluster")
    }
}

const config = parseConfig();

const confluentProvider = new confluent.Provider("conf-provider", {
    username: config.username,
    password: config.password,
})

const apiKey = new confluent.ApiKey("key", {
    environmentId: config.kafkaCluster.environmentId,
    clusterId: config.kafkaCluster.id,
}, { provider: confluentProvider })

const kafkaProvider = new kafka.Provider("kafka-provider", {
    bootstrapServers: [config.kafkaCluster.bootstrapServers.substring(config.kafkaCluster.bootstrapServers.lastIndexOf('/') + 1)],
    saslUsername: apiKey.key,
    saslPassword: apiKey.secret,
    saslMechanism: "plain",
    tlsEnabled: true,
})

export const topics = config.topicNames.map((topicName) => {
    return new kafka.Topic(`topic-${topicName}`, {
        name: topicName,
        partitions: 1,
        // We set replication factor to 3 regardless of the cluster availability
        // since that's the minimum required by confluent cloud:
        // https://github.com/Mongey/terraform-provider-kafka/issues/40#issuecomment-456897983
        replicationFactor: 3,
    }, { provider: kafkaProvider })
})