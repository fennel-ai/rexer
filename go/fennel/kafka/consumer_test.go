package kafka

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/resource"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)
import "github.com/alexflint/go-arg"

type KafkaBenchmarkArgs struct {
	MskKafkaServer   string `arg:"--msk-kafka-server,env:MSK_KAFKA_SERVER_ADDRESS" json:"msk_kafka_server,omitempty"`
	MskKafkaUsername string `arg:"--msk-kafka-user,env:MSK_KAFKA_USERNAME" json:"msk_kafka_username,omitempty"`
	MskKafkaPassword string `arg:"--msk-kafka-password,env:MSK_KAFKA_PASSWORD" json:"msk_kafka_password,omitempty"`
}

func benchmarkConfluent(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()

	var args KafkaBenchmarkArgs
	_ = arg.Parse(&args)
	c, err := RemoteConsumerConfig{
		BootstrapServer: args.MskKafkaServer,
		Username: args.MskKafkaUsername,
		Password: args.MskKafkaPassword,
		SaslMechanism: SaslScramSha512Mechanism,
		ConsumerConfig: ConsumerConfig{
			Topic: "benchmark-topic2",
			GroupID: "confluent-consumer",
			Scope: resource.NewPlaneScope(ftypes.RealmID(10000001)),
			OffsetPolicy: DefaultOffsetPolicy,
		},
	}.Materialize()
	assert.NoError(b, err)

	data := make([][]byte, 10_000)

	b.StartTimer()

	consumer := c.(RemoteConsumer)
	for true {
		d, err := consumer.ReadBatch(ctx, 1000, 10 * time.Second)
		assert.NoError(b, err)
		data = append(data, d...)

		log, err := consumer.Backlog()
		if err != nil {
			fmt.Printf("warn: backlog failed: %v\n", err)
		} else {
			if log == 0 {
				break
			}
			fmt.Printf("lag: %d\n", log)
		}
		_, err = consumer.Commit()
		if err != nil {
			fmt.Printf("warn: commit failed: %v\n", err)
		}
	}
	_ = consumer.Close()
	b.StopTimer()
}

func benchmarkSegment(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()

	var args KafkaBenchmarkArgs
	_ = arg.Parse(&args)
	c, err := RemoteConsumerConfig{
		BootstrapServer: args.MskKafkaServer,
		Username: args.MskKafkaUsername,
		Password: args.MskKafkaPassword,
		SaslMechanism: SaslScramSha512Mechanism,
		ConsumerConfig: ConsumerConfig{
			Topic: "benchmark-topic2",
			GroupID: "confluent-consumer",
			Scope: resource.NewPlaneScope(ftypes.RealmID(10000001)),
			OffsetPolicy: DefaultOffsetPolicy,
		},
	}.Materialize()
	assert.NoError(b, err)

	data := make([][]byte, 10_000)

	b.StartTimer()

	consumer := c.(RemoteConsumer)
	for true {
		d, err := consumer.ReadBatch(ctx, 1000, 10 * time.Second)
		assert.NoError(b, err)
		data = append(data, d...)

		log, err := consumer.Backlog()
		if err != nil {
			fmt.Printf("warn: backlog failed: %v\n", err)
		} else {
			if log == 0 {
				break
			}
			fmt.Printf("lag: %d\n", log)
		}
		_, err = consumer.Commit()
		if err != nil {
			fmt.Printf("warn: commit failed: %v\n", err)
		}
	}
	b.StopTimer()
}

func BenchmarkConsumers(b *testing.B) {
	// setup args for remote kafka cluster
	var args KafkaBenchmarkArgs
	_ = arg.Parse(&args)
	scope := resource.NewPlaneScope(ftypes.RealmID(10000001))
	topic := "benchmark-topic2"

	r, err := RemoteProducerConfig{
		Topic: topic,
		BootstrapServer: args.MskKafkaServer,
		Username: args.MskKafkaUsername,
		Password: args.MskKafkaPassword,
		SaslMechanism: SaslScramSha512Mechanism,
		Scope: resource.NewPlaneScope(ftypes.RealmID(10000001)),
	}.Materialize()

	defer func() {
		configmap := &kafka.ConfigMap{
			"bootstrap.servers": args.MskKafkaServer,
			"sasl.username":     args.MskKafkaUsername,
			"sasl.password":     args.MskKafkaPassword,
			"security.protocol": SecurityProtocol,
			"sasl.mechanisms":   SaslScramSha512Mechanism,
		}
		admin, err := kafka.NewAdminClient(configmap)
		if err != nil {
			panic(err)
		}
		_, err = admin.DeleteTopics(context.Background(), []string{scope.PrefixedName(topic)})
		if err != nil {
			panic(err)
		}
	}()

	assert.NoError(b, err)
	ctx := context.Background()
	producer := r.(RemoteProducer)
	data := make([][]byte, 10_000)
	for i := 0; i < 10_000; i++ {
		d := []byte(fmt.Sprintf("i_%s", utils.RandString(3)))
		err := producer.Log(ctx, d, nil)
		assert.NoError(b, err)
		data[i] = d
	}

	// create and write 10000 messages to a topic

	// each benchmark will create a consumer group with a different name and try to read from it
	b.Run("confluent-go", benchmarkConfluent)
	//b.Run("segment-go", benchmarkConfluent)
	//b.Run("shopify-go", benchmarkConfluent)
}