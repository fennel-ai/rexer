package kafka

import (
	"context"
	"fmt"
	"reflect"
	"time"

	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/resource"
	"fennel/tier"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func CreateMockKafka(tierID ftypes.RealmID) (map[string]fkafka.FProducer, tier.KafkaConsumerCreator, error) {
	brokerMap := make(map[string]*fkafka.MockBroker)
	producers := make(map[string]fkafka.FProducer)
	scope := resource.NewTierScope(tierID)
	allTopics := append(fkafka.ALL_CONFLUENT_TOPICS, fkafka.ALL_MSK_TOPICS...)
	for _, topic := range allTopics {
		if reflect.TypeOf(scope) != reflect.TypeOf(topic.Scope) {
			continue
		}
		broker := fkafka.NewMockTopicBroker()
		brokerMap[topic.Topic] = &broker
		prodConfig := fkafka.MockProducerConfig{
			Broker: &broker,
			Topic:  scope.PrefixedName(topic.Topic),
			Scope:  scope,
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
			Topic:   config.Topic,
			GroupID: config.GroupID,
			Scope:   config.Scope,
		}.Materialize()
		if err != nil {
			return nil, err
		}
		return kConsumer.(fkafka.FConsumer), nil
	}
	return producers, consumerCreator, nil
}

func SetupKafkaTopics(scope resource.Scope, host, username, password, saslMechanism string, topics []fkafka.TopicConf) error {
	var names []string
	for _, topic := range topics {
		if reflect.TypeOf(scope) == reflect.TypeOf(topic.Scope) {
			names = append(names, scope.PrefixedName(topic.Topic))
		}
	}

	if len(names) == 0 {
		// no topics to create
		return nil
	}
	
	// Create admin client
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(host, username, password, saslMechanism))
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
