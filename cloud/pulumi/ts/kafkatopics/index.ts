import * as pulumi from "@pulumi/pulumi";
import * as kafka from "@pulumi/kafka";

export const plugins = {
    "kafka": "v3.1.2",
}

export type topicConf = {
    name: string,
    partitions?: number,
    replicationFactor?: number,
}

const DEFAULT_PARTITIONS = 1;
const DEFAULT_REPLICATION_FACTOR = 3;

export type inputType = {
    apiKey: string,
    apiSecret: pulumi.Output<string>
    topics: topicConf[],
    bootstrapServer: string,
    protect: boolean,
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

    const topics = input.topics.map((topic) => {
        return new kafka.Topic(`topic-${topic.name}`, {
            name: topic.name,
            partitions: topic.partitions || DEFAULT_PARTITIONS,
            // We set replication factor to 3 regardless of the cluster availability
            // since that's the minimum required by confluent cloud:
            // https://github.com/Mongey/terraform-provider-kafka/issues/40#issuecomment-456897983
            replicationFactor: topic.replicationFactor || DEFAULT_REPLICATION_FACTOR,
        }, { provider: kafkaProvider, protect: input.protect })
    })

    const output: outputType = {
        topics
    }
    return output
}
