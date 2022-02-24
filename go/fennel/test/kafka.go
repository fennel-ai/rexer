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

func createMockKafka(tierID ftypes.TierID) (map[string]fkafka.FProducer, tier.KafkaConsumerCreator, error) {
	scope := resource.NewTierScope(1, tierID)
	brokerMap := make(map[string]*fkafka.MockBroker)
	producers := make(map[string]fkafka.FProducer)
	for _, topic := range fkafka.ALL_TOPICS {
		broker := fkafka.NewMockTopicBroker()
		brokerMap[topic] = &broker
		prodConfig := fkafka.MockProducerConfig{
			Broker: &broker,
			Topic:  scope.PrefixedName(topic),
		}
		kProducer, err := prodConfig.Materialize()
		if err != nil {
			return nil, nil, err
		}
		producers[topic] = kProducer.(fkafka.FProducer)
	}
	consumerCreator := func(topic, groupID, offsetPolicy string) (fkafka.FConsumer, error) {
		broker, ok := brokerMap[topic]
		if !ok {
			return nil, fmt.Errorf("unrecognized topic: %v", topic)
		}
		kConsumer, err := fkafka.MockConsumerConfig{
			Broker:  broker,
			Topic:   scope.PrefixedName(topic),
			GroupID: groupID,
		}.Materialize()
		if err != nil {
			return nil, err
		}
		return kConsumer.(fkafka.FConsumer), nil
	}
	return producers, consumerCreator, nil
}

func setupKafkaTopics(tierID ftypes.TierID, host, username, password string, topics []string) error {
	scope := resource.NewTierScope(1, tierID)
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = scope.PrefixedName(topic)
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

func teardownKafkaTopics(tierID ftypes.TierID, host, username, password string, topics []string) error {
	scope := resource.NewTierScope(1, tierID)
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = scope.PrefixedName(topic)
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
