//go:build !integration

package test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	testhangar "fennel/hangar/test"
	"fennel/kafka"
	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	libnitrous "fennel/lib/nitrous"
	"fennel/nitrous"
	"fennel/resource"

	"github.com/raulk/clock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type TestNitrous struct {
	nitrous.Nitrous
	broker *fkafka.MockBroker
}

func NewTestNitrous(t *testing.T) TestNitrous {
	rand.Seed(time.Now().UnixNano())
	planeId := ftypes.RealmID(rand.Uint32())
	// TODO: use db Hangar instead of in-memory hangar.
	// Currently, using db hangar leads to a test failure under -race flag
	// in the controller/counter package.
	// db, err := db.NewHangar(planeId, t.TempDir(), 1<<10, encoders.Default())
	// assert.NoError(t, err)
	db := testhangar.NewInMemoryHangar(planeId)
	broker := fkafka.NewMockTopicBroker()
	logger, err := zap.NewDevelopment()
	assert.NoError(t, err)
	n := nitrous.Nitrous{
		PlaneID: planeId,
		Logger:  logger,
		Store:   db,
		Clock:   clock.New(),
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
	t.Setenv("PLANE_ID", fmt.Sprintf("%d", planeId))
	return TestNitrous{
		Nitrous: n,
		broker:  &broker,
	}
}

func (tn TestNitrous) NewBinlogProducer(t *testing.T) kafka.FProducer {
	scope := resource.NewPlaneScope(tn.Nitrous.PlaneID)
	mockConfig := fkafka.MockProducerConfig{
		Broker: tn.broker,
		Topic:  libnitrous.BINLOG_KAFKA_TOPIC,
		Scope:  scope,
	}
	p, err := mockConfig.Materialize()
	assert.NoError(t, err)
	return p.(kafka.FProducer)
}
