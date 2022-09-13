//go:build integration

package test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	libnitrous "fennel/lib/nitrous"
	"fennel/nitrous"
	"fennel/resource"
	"fennel/test/kafka"

	"github.com/alexflint/go-arg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestNitrous struct {
	nitrous.Nitrous
	args nitrous.NitrousArgs
}

func NewTestNitrous[TB testing.TB](t TB) TestNitrous {
	rand.Seed(time.Now().UnixNano())
	planeId := ftypes.RealmID(rand.Uint32())
	var flags nitrous.NitrousArgs
	// Parse flags / environment variables.
	arg.Parse(&flags)
	flags.Dev = true
	flags.PlaneID = planeId
	flags.BadgerDir = t.TempDir()
	flags.BadgerBlockCacheMB = 1000
	flags.RistrettoMaxCost = 1000
	flags.RistrettoAvgCost = 1
	flags.Identity = "localhost"
	p, err := nitrous.CreateFromArgs(flags)
	require.NoError(t, err)
	t.Setenv("PLANE_ID", fmt.Sprintf("%d", p.PlaneID))
	// Create plane-level kafka topics.
	scope := resource.NewPlaneScope(p.PlaneID)
	err = kafka.SetupKafkaTopics(scope, flags.MskKafkaServer, flags.MskKafkaUsername, flags.MskKafkaPassword, fkafka.SaslScramSha512Mechanism, fkafka.ALL_MSK_TOPICS)
	assert.NoError(t, err)

	return TestNitrous{
		Nitrous: p,
		args:    flags,
	}
}

func (tn TestNitrous) NewBinlogProducer(t *testing.T) fkafka.FProducer {
	scope := resource.NewPlaneScope(tn.Nitrous.PlaneID)
	config := fkafka.RemoteProducerConfig{
		Scope:           scope,
		Topic:           libnitrous.BINLOG_KAFKA_TOPIC,
		BootstrapServer: tn.args.MskKafkaServer,
		Username:        tn.args.MskKafkaUsername,
		Password:        tn.args.MskKafkaPassword,
		SaslMechanism:   fkafka.SaslScramSha512Mechanism,
	}
	p, err := config.Materialize()
	require.NoError(t, err)
	return p.(fkafka.FProducer)
}

func (tn TestNitrous) NewReqLogProducer(t *testing.T) fkafka.FProducer {
	scope := resource.NewPlaneScope(tn.Nitrous.PlaneID)
	config := fkafka.RemoteProducerConfig{
		Scope:           scope,
		Topic:           libnitrous.REQS_KAFKA_TOPIC,
		BootstrapServer: tn.args.MskKafkaServer,
		Username:        tn.args.MskKafkaUsername,
		Password:        tn.args.MskKafkaPassword,
		SaslMechanism:   fkafka.SaslScramSha512Mechanism,
	}
	p, err := config.Materialize()
	require.NoError(t, err)
	return p.(fkafka.FProducer)
}