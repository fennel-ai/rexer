package kafka

import (
	"context"
	"time"

	"fennel/lib/action"
	"fennel/lib/feature"
	"fennel/lib/profile"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

const (
	SecurityProtocol    = "SASL_SSL"
	SaslMechanism       = "PLAIN"
	DefaultOffsetPolicy = "earliest"
)

type FConsumer interface {
	resource.Resource
	Read(ctx context.Context, timeout time.Duration) ([]byte, error)
	ReadProto(ctx context.Context, message proto.Message, timeout time.Duration) error
	ReadBatch(ctx context.Context, upto int, timeout time.Duration) ([][]byte, error)
	Backlog() (int, error)
	Commit() error
	AsyncCommit() chan error
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
	feature.KAFKA_TOPIC_NAME,
	profile.PROFILELOG_KAFKA_TOPIC,
}

func ConfigMap(server, username, password string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers": server,
		"sasl.username":     username,
		"sasl.password":     password,
		"security.protocol": SecurityProtocol,
		"sasl.mechanisms":   SaslMechanism,
	}
}
