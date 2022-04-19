import * as pulumi from "@pulumi/pulumi";
import * as kafka from "@pulumi/kafka";

export const plugins = {
    "kafka": "v3.1.2",
}

export type inputType = {
    apiKey: string,
    apiSecret: pulumi.Output<string>
    topicNames: string[],
    bootstrapServer: string,
}

export type outputType = {
    topics: kafka.Topic[]
}

export const setup = async (input: inputType) => {
    const kafkaProvider = new kafka.Provider("kafka-provider", {
        bootstrapServers: [input.bootstrapServer],
        saslUsername: input.apiKey,
        saslPassword: input.apiSecret,
        saslMechanism: "plain",
        tlsEnabled: true,
    })

    const topics = input.topicNames.map((topicName) => {
        return new kafka.Topic(`topic-${topicName}`, {
            name: topicName,
            partitions: 1,
            // We set replication factor to 3 regardless of the cluster availability
            // since that's the minimum required by confluent cloud:
            // https://github.com/Mongey/terraform-provider-kafka/issues/40#issuecomment-456897983
            replicationFactor: 3,
        }, { provider: kafkaProvider })
    })

    const output: outputType = {
        topics
    }
    return output
}
