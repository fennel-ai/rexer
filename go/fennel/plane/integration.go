//go:build integration

package plane

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"fennel/kafka"
	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/lib/nitrous"
	"fennel/resource"
	"fennel/test"

	"github.com/alexflint/go-arg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestPlane struct {
	Plane
	args PlaneArgs
}

func NewTestPlane(t *testing.T) TestPlane {
	rand.Seed(time.Now().UnixNano())
	planeId := ftypes.RealmID(rand.Uint32())
	var flags PlaneArgs
	// Parse flags / environment variables.
	arg.Parse(&flags)
	flags.Dev = true
	flags.PlaneID = planeId
	flags.BadgerDir = t.TempDir()
	flags.BadgerBlockCacheMB = 1000
	flags.RistrettoMaxCost = 1000
	flags.RistrettoAvgCost = 1
	p, err := CreateFromArgs(flags)
	require.NoError(t, err)
	// Create the binlog kafka topic for this plane.
	os.Setenv("PLANE_ID", fmt.Sprintf("%d", p.ID))
	// Create plane-level kafka topics.
	scope := resource.NewPlaneScope(p.ID)
	err = test.SetupKafkaTopics(scope, flags.KafkaServer, flags.KafkaUsername, flags.KafkaPassword)
	assert.NoError(t, err)

	return TestPlane{
		Plane: p,
		args:  flags,
	}
}

func (tp TestPlane) NewBinlogProducer(t *testing.T) kafka.FProducer {
	scope := resource.NewPlaneScope(tp.ID)
	config := fkafka.RemoteProducerConfig{
		Scope:           scope,
		Topic:           nitrous.BINLOG_KAFKA_TOPIC,
		BootstrapServer: tp.args.KafkaServer,
		Username:        tp.args.KafkaUsername,
		Password:        tp.args.KafkaPassword,
	}
	p, err := config.Materialize()
	require.NoError(t, err)
	return p.(kafka.FProducer)
}
