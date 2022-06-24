package plane

import (
	"fennel/hangar/db"
	"fennel/hangar/encoders"
	"fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/resource"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type TestPlane struct {
	Plane
	broker *kafka.MockBroker
}

func NewTestPlane(t *testing.T) TestPlane {
	planeId := ftypes.RealmID(1)
	db, err := db.NewHangar(planeId, t.TempDir(), 1<<10, encoders.Default())
	assert.NoError(t, err)
	broker := kafka.NewMockTopicBroker()
	p := Plane{
		ID:     planeId,
		Logger: zap.NewNop(),
		Store:  db,
		KafkaConsumerFactory: func(config kafka.ConsumerConfig) (kafka.FConsumer, error) {
			mockConfig := kafka.MockConsumerConfig{
				Broker:  &broker,
				Topic:   config.Topic,
				GroupID: config.GroupID,
				Scope:   resource.NewPlaneScope(planeId),
			}
			consumer, err := mockConfig.Materialize()
			return consumer.(kafka.FConsumer), err
		},
	}
	return TestPlane{
		Plane:  p,
		broker: &broker,
	}
}

func (tp *TestPlane) NewProducer(t *testing.T, topic string) kafka.FProducer {
	config := kafka.MockProducerConfig{
		Broker: tp.broker,
		Scope:  resource.NewPlaneScope(tp.ID),
		Topic:  topic,
	}
	producer, err := config.Materialize()
	assert.NoError(t, err)
	return producer.(kafka.FProducer)
}
