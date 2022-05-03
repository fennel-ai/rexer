package test

import (
	"context"
	"fmt"
	"time"

	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/resource"
	"fennel/tier"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func createMockKafka(tierID ftypes.RealmID) (map[string]fkafka.FProducer, tier.KafkaConsumerCreator, error) {
	scope := resource.NewTierScope(tierID)
	brokerMap := make(map[string]*fkafka.MockBroker)
	producers := make(map[string]fkafka.FProducer)
	for _, topic := range fkafka.ALL_TOPICS {
		broker := fkafka.NewMockTopicBroker()
		brokerMap[topic.Topic] = &broker
		prodConfig := fkafka.MockProducerConfig{
			Broker: &broker,
			Topic:  scope.PrefixedName(topic.Topic),
		}
		kProducer, err := prodConfig.Materialize()
		if err != nil {
			return nil, nil, err
		}
		producers[topic.Topic] = kProducer.(fkafka.FProducer)
	}
	consumerCreator := func(config fkafka.ConsumerConfig) (fkafka.FConsumer, error) {
		broker, ok := brokerMap[config.Topic]
		if !ok {
			return nil, fmt.Errorf("unrecognized topic: %v", config.Topic)
		}
		kConsumer, err := fkafka.MockConsumerConfig{
			Broker:  broker,
			Topic:   scope.PrefixedName(config.Topic),
			GroupID: config.GroupID,
		}.Materialize()
		if err != nil {
			return nil, err
		}
		return kConsumer.(fkafka.FConsumer), nil
	}
	return producers, consumerCreator, nil
}

func setupKafkaTopics(tierID ftypes.RealmID, host, username, password string, topics []fkafka.TopicConf) error {
	scope := resource.NewTierScope(tierID)
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = scope.PrefixedName(topic.Topic)
	}
	// Create admin client
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(host, username, password))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// now create the topics
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	specs := make([]kafka.TopicSpecification, len(names))
	for i, name := range names {
		specs[i] = kafka.TopicSpecification{
			Topic:             name,
			NumPartitions:     1,
			ReplicationFactor: 0,
		}
	}
	results, err := c.CreateTopics(ctx, specs)
	if err != nil {
		return fmt.Errorf("failed to create topics: %v", err)
	}
	for _, tr := range results {
		if tr.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf(tr.Error.Error())
		}
	}
	return nil
}

func teardownKafkaTopics(tierID ftypes.RealmID, host, username, password string, topics []fkafka.TopicConf) error {
	scope := resource.NewTierScope(tierID)
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = scope.PrefixedName(topic.Topic)
	}
	// Create admin client.
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(host, username, password))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// delete any existing topics of these names
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = c.DeleteTopics(ctx, names)
	return err
}
