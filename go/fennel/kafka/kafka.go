package kafka

import (
	"context"
	"encoding/json"
	"fennel/data/lib"
	"fmt"
	"log"
	"time"

	"fennel/data/server/actions"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

const (
	// bootstrap_server = "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092"
	// username         = "DXGQTONRSCJ7YC2W"
	// password        = "s1TAmKoJ7WnbJusVMPlgvKbYszD6lE50789bM1Y6aTlJNtRjThhhPR8+LeaY6Mqi"

	securityProtocol = "SASL_SSL"
	saslMechanism    = "PLAIN"
)

type ClientConfig struct {
	BootstrapServer string `json:"bootstrap_server"`
	Username        string `json:"username,omitempty"`
	Password        string `json:"password,omitempty"`
}

func (c *ClientConfig) Parse(b []byte) (err error) {
	err = json.Unmarshal(b, c)
	return
}

func (c *ClientConfig) genConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers": c.BootstrapServer,
		"sasl.username":     c.Username,
		"sasl.password":     c.Password,
		"security.protocol": securityProtocol,
		"sasl.mechanisms":   saslMechanism,
	}
}

func (c *ClientConfig) NewAdminClient() (*kafka.AdminClient, error) {
	return kafka.NewAdminClient(c.genConfigMap())
}

func (c *ClientConfig) NewActionProducer(topicId string) (*KafkaActionProducer, error) {
	configmap := c.genConfigMap()
	producer, err := kafka.NewProducer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kafka producer for topic [%s]: %v", topicId, err)
	}
	// Delivery report handler for produced messages
	// This starts a go-routine that goes through all "delivery reports" for sends
	// as they arrive and logs if any of those are failing
	go func() {
		for e := range producer.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				log.Printf("[ERROR] Kafka send failed. Event: %v", e.String())
			}
		}
	}()
	return &KafkaActionProducer{
		kafkaProducer: producer,
		topicId:       topicId,
	}, err
}

func (c *ClientConfig) NewActionConsumer(groupId, topicId string) (*KafkaActionConsumer, error) {
	configmap := c.genConfigMap()
	// consumer specific configs.
	configmap.SetKey("group.id", groupId)
	configmap.SetKey("auto.offset.reset", "earliest")
	consumer, err := kafka.NewConsumer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
	}
	err = consumer.Subscribe(topicId, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic [%s]: %v", topicId, err)
	}
	return &KafkaActionConsumer{
		kafkaConsumer: consumer,
	}, err
}

var _ actions.ActionProducer = (*KafkaActionProducer)(nil)
var _ actions.ActionConsumer = (*KafkaActionConsumer)(nil)

type KafkaActionProducer struct {
	kafkaProducer *kafka.Producer
	topicId       string
}

func (ap *KafkaActionProducer) LogAction(action *lib.ProtoAction) error {
	value, err := proto.Marshal(action)
	if err != nil {
		return fmt.Errorf("failed to serialize action to proto: %v", err)
	}
	message := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &ap.topicId, Partition: kafka.PartitionAny},
		Value:          value,
	}
	return ap.kafkaProducer.Produce(&message, nil)
}

func (ap *KafkaActionProducer) Flush(timeout time.Duration) int {
	return ap.kafkaProducer.Flush(int(timeout.Milliseconds()))
}

type KafkaActionConsumer struct {
	kafkaConsumer *kafka.Consumer
}

func (ac *KafkaActionConsumer) ReadActionMessage() (*lib.ProtoAction, error) {
	msg, err := ac.kafkaConsumer.ReadMessage(-1)
	if err != nil {
		return nil, fmt.Errorf("failed to read msg from kafka: %v", err)
	}
	action := &lib.ProtoAction{}
	err = proto.Unmarshal(msg.Value, action)
	if err != nil {
		return nil, fmt.Errorf("failed to parse msg from kafka to action: %v", err)
	}
	return action, nil
}

// TODO: move to a test-only file or package.
func (config *ClientConfig) SetupTestTopic() (string, error) {
	log.Println("Setting up test kafka topic")

	// Create admin client.
	c, err := config.NewAdminClient()
	if err != nil {
		return "", fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// TODO: Generate a random topic name.
	topicname := "test_actionlog_abhay"

	// First, delete any existing topics of this name
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// we ignore results/errors because sometimes the topic may not exist
	_, _ = c.DeleteTopics(ctx, []string{topicname})
	// now recreate the topic
	results, err := c.CreateTopics(ctx, []kafka.TopicSpecification{
		{
			Topic:         topicname,
			NumPartitions: 1,
		},
	})
	if err != nil {
		return "", fmt.Errorf("failed to create topic [%s]: %v", topicname, err)
	}
	for _, tr := range results {
		if tr.Error.Code() != kafka.ErrNoError {
			return "", fmt.Errorf(tr.Error.Error())
		}
	}
	log.Printf("Done setting up kafka topic: %s", topicname)
	return topicname, nil
}
