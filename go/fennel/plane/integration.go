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
	return TestPlane{
		Plane: p,
		args:  flags,
	}
}

func (tp TestPlane) NewProducer(t *testing.T, topic string) kafka.FProducer {
	scope := resource.NewPlaneScope(tp.Plane.ID)
	// Create the kafka topic.
	// TODO: This should be separated out from tier creation?
	rand.Seed(time.Now().UnixNano())
	tierId := ftypes.RealmID(rand.Uint32())
	err := test.SetupKafkaTopics(
		tierId, tp.ID, tp.args.KafkaServer, tp.args.KafkaUsername, tp.args.KafkaPassword,
		[]kafka.TopicConf{{Scope: scope, Topic: topic}})
	assert.NoError(t, err)

	config := fkafka.RemoteProducerConfig{
		Scope:           scope,
		Topic:           topic,
		BootstrapServer: tp.args.KafkaServer,
		Username:        tp.args.KafkaUsername,
		Password:        tp.args.KafkaPassword,
	}
	p, err := config.Materialize()
	require.NoError(t, err)
	return p.(kafka.FProducer)
}
