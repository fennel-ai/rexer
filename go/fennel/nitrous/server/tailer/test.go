package tailer

import (
	"fennel/kafka"
	"fennel/plane"
	"fennel/resource"
	"sync"
	"time"
)

func NewTestTailer(plane plane.Plane, topic string) *Tailer {
	consumer, _ := plane.KafkaConsumerFactory(kafka.ConsumerConfig{
		Scope:        resource.NewPlaneScope(plane.ID),
		Topic:        topic,
		GroupID:      "hello-world-group",
		OffsetPolicy: "earliest",
	})
	return &Tailer{
		nil,
		plane,
		consumer.(kafka.FConsumer),
		[]byte("default-offsets-kg"),
		nil,
		100 * time.Millisecond, // Short poll timeout of 100ms for tests.
		&sync.RWMutex{},
	}
}
