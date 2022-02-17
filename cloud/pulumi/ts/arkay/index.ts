import setupTier from './tier'

setupTier(
    {
        tierId: 1,
        kafkaCluster: {
            bootstrapServers: "SASL_SSL://pkc-pgq85.us-west-2.aws.confluent.cloud:9092",
            environmentId: "env-k36j2",
            id: "lkc-12r9x3",
        },
        topicNames: ["my-favorite-topic"],
    },
    false,
)