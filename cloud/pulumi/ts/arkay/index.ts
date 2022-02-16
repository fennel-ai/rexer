import setupTier from './tier'

setupTier(
    {
        tierId: 1,
        kafkaCluster: {
            bootstrapServers: "SASL_SSL://pkc-pgq85.us-west-2.aws.confluent.cloud:9092",
            environmentId: "env-rpjx9",
            id: "lkc-v7p7yz",
        },
        topicNames: ["my-favorite-topic"],
    },
    true,
)