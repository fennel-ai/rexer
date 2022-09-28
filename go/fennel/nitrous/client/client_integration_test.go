//go:build integration

package client_test

import (
	"context"
	fkafka "fennel/kafka"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	libnitrous "fennel/lib/nitrous"
	"fennel/lib/utils"
	"fennel/lib/value"
	fnitrous "fennel/nitrous"
	"fennel/nitrous/client"
	"fennel/resource"
	"fennel/test/kafka"
	"fennel/test/nitrous"
	"fmt"
	"github.com/alexflint/go-arg"
	confKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func topicProducer(args fnitrous.NitrousArgs, scope resource.Scope, topic string) (fkafka.FProducer, error) {
	config := fkafka.RemoteProducerConfig{
		Scope: scope,
		Topic: topic,
		BootstrapServer: args.MskKafkaServer,
		Username: args.MskKafkaUsername,
		Password: args.MskKafkaPassword,
		SaslMechanism: fkafka.SaslScramSha512Mechanism,
	}
	p, err := config.Materialize()
	return p.(fkafka.FProducer), err
}

func TestPushToShardedNitrous(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	planeId := ftypes.RealmID(rand.Uint32())
	scope := resource.NewPlaneScope(planeId)
	var flags fnitrous.NitrousArgs
	// Parse flags / environment variables.
	err := arg.Parse(&flags)
	flags.Dev = true
	flags.PlaneID = planeId
	flags.GravelDir = t.TempDir()
	flags.BadgerBlockCacheMB = 1000
	flags.RistrettoMaxCost = 1000
	flags.RistrettoAvgCost = 1

	// We will configure multiple partitions, to which the client will push based on the hash of the group keys
	flags.BinPartitions = 2
	flags.Identity = "localhost"
	p, err := fnitrous.CreateFromArgs(flags)
	require.NoError(t, err)
	t.Setenv("PLANE_ID", fmt.Sprintf("%d", p.PlaneID))

	// we need to create only the nitrous based topics
	topics := make([]confKafka.TopicSpecification, 0)
	topics = append(topics, confKafka.TopicSpecification{
		Topic: scope.PrefixedName(libnitrous.BINLOG_KAFKA_TOPIC),
		NumPartitions: 2,
		ReplicationFactor: 0,
	})
	topics = append(topics, confKafka.TopicSpecification{
		Topic: scope.PrefixedName(libnitrous.REQS_KAFKA_TOPIC),
		NumPartitions: 1,
		ReplicationFactor: 0,
	})
	topics = append(topics, confKafka.TopicSpecification{
		Topic: scope.PrefixedName(libnitrous.AGGR_CONF_KAFKA_TOPIC),
		NumPartitions: 1,
		ReplicationFactor: 0,
	})

	err = kafka.SetupKafkaTopicsFromSpec(flags.MskKafkaServer, flags.MskKafkaUsername, flags.MskKafkaPassword, fkafka.SaslScramSha512Mechanism, topics)
	assert.NoError(t, err)

	s, addr := nitrous.StartNitrousServer(t, p)

	// Create client.
	binlogProducer, err := topicProducer(flags, scope, libnitrous.BINLOG_KAFKA_TOPIC)
	assert.NoError(t, err)
	reqslogProducer, err := topicProducer(flags, scope, libnitrous.REQS_KAFKA_TOPIC)
	assert.NoError(t, err)
	aggrConfProducer, err := topicProducer(flags, scope, libnitrous.AGGR_CONF_KAFKA_TOPIC)
	assert.NoError(t, err)
	cfg := client.NitrousClientConfig{
		TierID:         0,
		ServerAddr:     addr.String(),
		BinlogProducer: binlogProducer,
		BinlogPartitions: 2,
		ReqsLogProducer: reqslogProducer,
		AggregateConfProducer: aggrConfProducer,
	}
	res, err := cfg.Materialize()
	assert.NoError(t, err)
	nc, ok := res.(client.NitrousClient)
	assert.True(t, ok)

	// Define a new aggregate on nitrous.
	aggId := ftypes.AggId(21)
	opts := aggregate.Options{
		AggType: "sum",
		Durations: []uint32{
			24 * 3600,
		},
	}
	ctx := context.Background()
	err = nc.CreateAggregate(ctx, aggId, opts)
	require.NoError(t, err)

	waitToConsume := func() {
		count := 0
		for count < 3 {
			// Assuming that nitrous tails the log every 100 ms in tests.
			time.Sleep(s.GetBinlogPollTimeout())
			lag, err := nc.GetLag(ctx)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			assert.NoError(t, err)
			t.Logf("Current lag: %d", lag)
			if lag == 0 {
				count++
			}
		}
		// It is possible for the lag to be zero but the event to not have
		// been processed yet. Sleep some more to reduce the likelihood of
		// that happening.
		time.Sleep(1 * time.Second)
	}

	// Wait till the binlog lag is 0 before sending any events for this aggregate.
	waitToConsume()

	kwargs := value.NewDict(nil)
	kwargs.Set("duration", value.Int(24*3600))

	// Get current value for the defined aggregate.
	out := make([]value.Value, 10_000, 10_000)
	err = nc.GetMulti(ctx, aggId, []value.Value{value.String("mygk")}, []value.Dict{kwargs}, out)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, out[0])

	// Create multiple events in the binlog - this is increase the likelihood of the messages going to different
	// topic partitions and while reading, it will validate that the read request was forwarded to the right
	// application shard and fetched
	expectedVals := make(map[value.String]value.Value, 10_000)
	events := make([]value.Value, 10_000)
	groupKeys := make([]value.Value, 10_000)
	kwargsL := make([]value.Dict, 10_000)
	for i := 0; i < 10_000; i++ {
		v := value.Int(rand.Uint32())
		groupkey := value.String(utils.RandString(6))
		event := value.NewDict(map[string]value.Value{
			"groupkey":  groupkey,
			"timestamp": value.Int(time.Now().Unix()),
			"value":     v,
		})
		events[i] = event
		groupKeys[i] = groupkey
		// use same kwargs
		kwargsL[i] = kwargs
		expectedVals[groupkey] = v
	}
	err = nc.Push(ctx, aggId, value.NewList(events...))
	assert.NoError(t, err)
	// Wait for the event to be consumed.
	waitToConsume()

	err = nc.GetMulti(ctx, aggId, groupKeys, kwargsL, out)
	assert.NoError(t, err)
	for i, actual := range out {
		assert.EqualValues(t, actual, expectedVals[groupKeys[i].(value.String)])
	}
}