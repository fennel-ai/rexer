//go:build !integration

package plane

import (
	"math/rand"
	"testing"
	"time"

	"fennel/hangar/db"
	"fennel/hangar/encoders"
	"fennel/kafka"
	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/resource"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type TestPlane struct {
	Plane
	broker *fkafka.MockBroker
}

func NewTestPlane(t *testing.T) TestPlane {
	rand.Seed(time.Now().UnixNano())
	planeId := ftypes.RealmID(rand.Uint32())
	db, err := db.NewHangar(planeId, t.TempDir(), 1<<10, encoders.Default())
	assert.NoError(t, err)
	broker := fkafka.NewMockTopicBroker()
	logger, err := zap.NewDevelopment()
	assert.NoError(t, err)
	p := Plane{
		ID:     planeId,
		Logger: logger,
		Store:  db,
		KafkaConsumerFactory: func(config fkafka.ConsumerConfig) (fkafka.FConsumer, error) {
			scope := resource.NewPlaneScope(planeId)
			mockConfig := fkafka.MockConsumerConfig{
				Broker:  &broker,
				Topic:   config.Topic,
				GroupID: config.GroupID,
				Scope:   scope,
			}
			consumer, err := mockConfig.Materialize()
			return consumer.(fkafka.FConsumer), err
		},
	}

	return TestPlane{
		Plane:  p,
		broker: &broker,
	}
}

func (tp TestPlane) NewProducer(t *testing.T, topic string) kafka.FProducer {
	scope := resource.NewPlaneScope(tp.Plane.ID)
	mockConfig := fkafka.MockProducerConfig{
		Broker: tp.broker,
		Topic:  topic,
		Scope:  scope,
	}
	p, err := mockConfig.Materialize()
	assert.NoError(t, err)
	return p.(kafka.FProducer)
}
