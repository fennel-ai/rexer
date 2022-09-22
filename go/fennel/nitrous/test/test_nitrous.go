//go:build !integration

package test

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"

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
	brokers map[string]*fkafka.MockBroker
}

func NewTestNitrous[TB testing.TB](t TB) TestNitrous {
	rand.Seed(time.Now().UnixNano())
	planeId := ftypes.RealmID(rand.Uint32())
	// Create a broker per topic. Ideally broker should handle this abstraction, but may be in the future
	brokers := make(map[string]*fkafka.MockBroker, 3)
	for _, topic := range []string{libnitrous.BINLOG_KAFKA_TOPIC, libnitrous.REQS_KAFKA_TOPIC, libnitrous.AGGR_CONF_KAFKA_TOPIC} {
		broker := fkafka.NewMockTopicBroker()
		brokers[topic] = &broker
	}
	logger, err := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
	assert.NoError(t, err)
	n := nitrous.Nitrous{
		PlaneID: planeId,
		Clock:   clock.New(),
		KafkaConsumerFactory: func(config fkafka.ConsumerConfig) (fkafka.FConsumer, error) {
			scope := resource.NewPlaneScope(planeId)
			mockConfig := fkafka.MockConsumerConfig{
				Broker:  brokers[config.Topic],
				Topic:   config.Topic,
				GroupID: config.GroupID,
				Scope:   scope,
			}
			consumer, err := mockConfig.Materialize()
			return consumer.(fkafka.FConsumer), err
		},
		DbDir: t.TempDir(),
		BinlogPartitions: 1,
	}
	t.Setenv("PLANE_ID", fmt.Sprintf("%d", planeId))
	return TestNitrous{
		Nitrous: n,
		brokers:  brokers,
	}
}

func (tn TestNitrous) NewBinlogProducer(t *testing.T) kafka.FProducer {
	scope := resource.NewPlaneScope(tn.Nitrous.PlaneID)
	mockConfig := fkafka.MockProducerConfig{
		Broker: tn.brokers[libnitrous.BINLOG_KAFKA_TOPIC],
		Topic:  libnitrous.BINLOG_KAFKA_TOPIC,
		Scope:  scope,
	}
	p, err := mockConfig.Materialize()
	assert.NoError(t, err)
	return p.(kafka.FProducer)
}

func (tn TestNitrous) NewReqLogProducer(t *testing.T) kafka.FProducer {
	scope := resource.NewPlaneScope(tn.Nitrous.PlaneID)
	mockConfig := fkafka.MockProducerConfig{
		Broker: tn.brokers[libnitrous.REQS_KAFKA_TOPIC],
		Topic:  libnitrous.REQS_KAFKA_TOPIC,
		Scope:  scope,
	}
	p, err := mockConfig.Materialize()
	assert.NoError(t, err)
	return p.(kafka.FProducer)
}

func (tn TestNitrous) NewAggregateConfProducer(t *testing.T) fkafka.FProducer {
	scope := resource.NewPlaneScope(tn.Nitrous.PlaneID)
	config := fkafka.MockProducerConfig{
		Scope:           scope,
		Topic:           libnitrous.AGGR_CONF_KAFKA_TOPIC,
		Broker: 	     tn.brokers[libnitrous.AGGR_CONF_KAFKA_TOPIC],
	}
	p, err := config.Materialize()
	require.NoError(t, err)
	return p.(fkafka.FProducer)
}