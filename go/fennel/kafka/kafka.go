package kafka

import (
	"fennel/resource"
	"google.golang.org/protobuf/proto"
)

const (
	SecurityProtocol = "SASL_SSL"
	SaslMechanism    = "PLAIN"
)

type FConsumer interface {
	resource.Resource
	Read(message proto.Message) error
}

type FProducer interface {
	resource.Resource
	Log(message proto.Message) error
}
