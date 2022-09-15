import * as pulumi from "@pulumi/pulumi";
import * as kafka from "@pulumi/kafka";

export const plugins = {
    "kafka": "v3.1.2",
}

export type topicConf = {
    name: string,
    partitions?: number,
    replicationFactor?: number,
    // Maximum time after which old messages (grouped as segments) are discarded to free up space
    //
    // Set -1 for "unlimited" retention
    retention_ms?: number,
    // Maximum size of a partition can grow to before old messages (grouped as segments) are discarded to free up space
    //
    // NOTE: This is configured per partition, therefore for a topic with multiple partitions, this should be
    // multiplied with the number of partitions to estimate the max occupied capacity
    //
    // Set -1 to configure no discards based on the size of the partition
    partition_retention_bytes?: number,
}

const DEFAULT_PARTITIONS = 1;
const DEFAULT_REPLICATION_FACTOR = 3;
const DEFAULT_MSK_REPLICATION_FACTOR = 2;

export type inputType = {
    apiKey: string,
    apiSecret: pulumi.Output<string>
    topics: topicConf[],
    bootstrapServer: string,
    protect: boolean,

    createInMsk?: boolean,
    mskApiKey?: string,
    mskApiSecret?: string,
    mskBootstrapServers?: string,
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
        const config = {
            "retention.ms": topic.retention_ms,
            "retention.bytes": topic.partition_retention_bytes,
        };
        return new kafka.Topic(`topic-${topic.name}`, {
            name: topic.name,
            partitions: topic.partitions || DEFAULT_PARTITIONS,
            // We set replication factor to 3 regardless of the cluster availability
            // since that's the minimum required by confluent cloud:
            // https://github.com/Mongey/terraform-provider-kafka/issues/40#issuecomment-456897983
            replicationFactor: topic.replicationFactor || DEFAULT_REPLICATION_FACTOR,
            config: config,
        }, { provider: kafkaProvider, protect: input.protect })
    })

    if (input.createInMsk) {
        const bootstrapServers = input.mskBootstrapServers!.split(",");
        const mskKafkaProvider = new kafka.Provider("msk-kafka-provider", {
            bootstrapServers: bootstrapServers,
            saslUsername: input.mskApiKey!,
            saslPassword: input.mskApiSecret!,
            saslMechanism: "scram-sha512",
        });

        const mskTopics = input.topics.map((topic) => {
            const config = {
                "retention.ms": topic.retention_ms,
                "retention.bytes": topic.partition_retention_bytes,
            };
            return new kafka.Topic(`msk-topic-${topic.name}`, {
                name: topic.name,
                partitions: topic.partitions || DEFAULT_PARTITIONS,
                // We set default replication factor of 2 since we have configured MSK cluster in 2 AZs
                replicationFactor: topic.replicationFactor || DEFAULT_MSK_REPLICATION_FACTOR,
                config: config,
            }, { provider: mskKafkaProvider, protect: input.protect })
        });
    }

    const output: outputType = {
        topics
    }
    return output
}
