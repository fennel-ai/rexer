//go:build integration

package client_test

import (
	"context"
	"net"
	"testing"
	"time"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/nitrous"
	"fennel/nitrous/client"
	"fennel/plane"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPush(t *testing.T) {
	plane := plane.NewTestPlane(t)

	// Start server.
	lis, err := net.Listen("tcp", ":0")
	assert.NoError(t, err)
	go nitrous.StartServer(plane.Plane, lis)

	// Create client.
	cfg := client.NitrousClientConfig{
		PlaneId:        plane.ID,
		ServerAddr:     lis.Addr().String(),
		BinlogProducer: plane.NewBinlogProducer(t),
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
		time.Sleep(10 * time.Second)
		for {
			lag, err := nc.GetLag(ctx)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			assert.NoError(t, err)
			plane.Logger.Info("Current lag", zap.Uint64("value", lag))
			if lag == 0 {
				break
			}
		}
		// It is possible for the lag to be zero but the event to not have
		// been processed yet. Sleep some more to reduce the likelihood of
		// that happening.
		time.Sleep(5 * time.Second)
	}

	// Wait till the binlog lag is 0 before sending any events for this aggregate.
	waitToConsume()

	// Get current value for the defined aggregate.
	out := make([]value.Value, 1)
	err = nc.GetMulti(ctx, aggId, 24*3600, []string{"mygk"}, out)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, out[0])

	// Push a new event for the aggregate.
	v := value.Int(124)
	groupkey := value.String("mygk")
	event := value.NewDict(map[string]value.Value{
		"groupkey":  groupkey,
		"timestamp": value.Int(time.Now().Unix()),
		"value":     v,
	})
	err = nc.Push(ctx, aggId, value.NewList(event))
	assert.NoError(t, err)
	// Wait for the event to be consumed.
	waitToConsume()
	// Now the value for the aggregate should be 124.
	err = nc.GetMulti(ctx, aggId, 24*3600, []string{groupkey.String()}, out)
	assert.NoError(t, err)
	assert.EqualValues(t, v, out[0])
}
