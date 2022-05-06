package kafka

import (
	"context"
	"time"

	"fennel/lib/action"
	"fennel/lib/counter"
	"fennel/lib/feature"
	"fennel/lib/profile"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

const (
	SecurityProtocol     = "SASL_SSL"
	SaslMechanism        = "PLAIN"
	DefaultOffsetPolicy  = "earliest"
	EarliestOffsetPolicy = "earliest"
	LatestOffsetPolicy   = "latest"
)

type FConsumer interface {
	resource.Resource
	Read(ctx context.Context, timeout time.Duration) ([]byte, error)
	ReadProto(ctx context.Context, message proto.Message, timeout time.Duration) error
	ReadBatch(ctx context.Context, upto int, timeout time.Duration) ([][]byte, error)
	Backlog() (int, error)
	Commit() (kafka.TopicPartitions, error)
	CommitOffsets(kafka.TopicPartitions) (kafka.TopicPartitions, error)
	Offsets() (kafka.TopicPartitions, error)
	GroupID() string
}

type FProducer interface {
	resource.Resource
	LogProto(ctx context.Context, message proto.Message, partitionKey []byte) error
	Log(ctx context.Context, message []byte, partitionKey []byte) error
	Flush(timeout time.Duration) error
}

type TopicConf struct {
	Topic    string
	PConfigs ProducerConfigs
}

var ALL_TOPICS = []TopicConf{
	{Topic: action.ACTIONLOG_KAFKA_TOPIC},
	// TODO: Deprecate `ACTIONLOG_JSON_KAFKA_TOPIC` once confluent go supports
	// producing and consuming schema versioned messages
	{Topic: action.ACTIONLOG_JSON_KAFKA_TOPIC},

	// NOTE: features kafka topic has multiple partitions.
	{
		Topic: feature.KAFKA_TOPIC_NAME,
		PConfigs: ProducerConfigs{
			// controls how many records are batched together and sent as a single request to the broker (one for each partition)
			// size in bytes; default=16384
			"batch.size=327680",
			// upper bound on the delay for batching of records
			// if the local queue has records of size `batch.size`, this delay is respected (sent ASAP), but in the absense
			// of load, this is the artifical delay introduced before sending batch of records; default=0 (sent immediately)
			"linger.ms=10",
		},
	},

	{Topic: profile.PROFILELOG_KAFKA_TOPIC},
	{
		Topic: counter.AGGREGATE_DELTA_TOPIC_NAME,
		PConfigs: ProducerConfigs{
			// Maximum size of one message; default=1000000
			"message.max.bytes=10000000",
		},
	},
	{Topic: counter.AGGREGATE_OFFLINE_TRANSFORM_TOPIC_NAME},
}

func ConfigMap(server, username, password string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers": server,
		"sasl.username":     username,
		"sasl.password":     password,
		"security.protocol": SecurityProtocol,
		"sasl.mechanisms":   SaslMechanism,
		// gather statistics every 1s
		"statistics.interval.ms": 1 * 1000,
		// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
		// default: 100000
		"queue.buffering.max.messages": 2000000,
		// default: 1048576
		// this has a higher priority than `queue.buffering.max.messages`
		"queue.buffering.max.kbytes": 2097152,
	}
}
