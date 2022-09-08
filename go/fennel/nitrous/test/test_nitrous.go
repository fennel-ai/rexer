//go:build !integration

package test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"fennel/hangar/db"
	"fennel/hangar/encoders"
	"fennel/kafka"
	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	libnitrous "fennel/lib/nitrous"
	"fennel/nitrous"
	"fennel/resource"

	"github.com/dgraph-io/badger/v3"
	"github.com/raulk/clock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type TestNitrous struct {
	nitrous.Nitrous
	broker *fkafka.MockBroker
}

func NewTestNitrous[TB testing.TB](t TB) TestNitrous {
	rand.Seed(time.Now().UnixNano())
	planeId := ftypes.RealmID(rand.Uint32())
	db, err := db.NewHangar(planeId, badger.DefaultOptions(t.TempDir()), encoders.Default())
	t.Cleanup(func() { _ = db.Teardown() })
	assert.NoError(t, err)
	broker := fkafka.NewMockTopicBroker()
	logger, err := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
	assert.NoError(t, err)
	n := nitrous.Nitrous{
		PlaneID: planeId,
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
