package kafka

import (
	"fennel/lib/action"
	"fennel/lib/feature"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

const (
	SecurityProtocol = "SASL_SSL"
	SaslMechanism    = "PLAIN"
)

type FConsumer interface {
	resource.Resource
	Read(message proto.Message) error
	Backlog() (int, error)
}

type FProducer interface {
	resource.Resource
	Log(message proto.Message) error
}

var ALL_TOPICS = []string{
	action.ACTIONLOG_KAFKA_TOPIC,
	feature.KAFKA_TOPIC_NAME,
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
