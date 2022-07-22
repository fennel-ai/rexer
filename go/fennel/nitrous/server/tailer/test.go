package tailer

import (
	"sync"
	"time"

	"fennel/kafka"
	"fennel/nitrous"
	"fennel/resource"

	"go.uber.org/atomic"
)

func NewTestTailer(n nitrous.Nitrous, topic string) *Tailer {
	consumer, _ := n.KafkaConsumerFactory(kafka.ConsumerConfig{
		Scope:        resource.NewPlaneScope(n.PlaneID),
		Topic:        topic,
		GroupID:      "hello-world-group",
		OffsetPolicy: "earliest",
	})
	return &Tailer{
		nil,
		n,
		consumer,
		[]byte("default-offsets-kg"),
		nil,
		5 * time.Second,
		atomic.NewBool(false),
		&sync.RWMutex{},
	}
}
