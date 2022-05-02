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

var ALL_TOPICS = []string{
	action.ACTIONLOG_KAFKA_TOPIC,
	// TODO: Deprecate `ACTIONLOG_JSON_KAFKA_TOPIC` once confluent go supports
	// producing and consuming schema versioned messages
	action.ACTIONLOG_JSON_KAFKA_TOPIC,

	// NOTE: features kafka topic has multiple partitions.
	feature.KAFKA_TOPIC_NAME,

	profile.PROFILELOG_KAFKA_TOPIC,
	counter.AGGREGATE_DELTA_TOPIC_NAME,
	counter.AGGREGATE_OFFLINE_TRANSFORM_TOPIC_NAME,
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
	}
}
