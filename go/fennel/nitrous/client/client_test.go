package client_test

import (
	"context"
	"testing"
	"time"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/nitrous/client"
	"fennel/nitrous/test"
	"fennel/test/nitrous"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPush(t *testing.T) {
	n := test.NewTestNitrous(t)
	server, addr := nitrous.StartNitrousServer(t, n.Nitrous)

	// Create client.
	cfg := client.NitrousClientConfig{
		TierID:         0,
		ServerAddr:     addr.String(),
		BinlogProducer: n.NewBinlogProducer(t),
		BinlogPartitions: 1,
		ReqsLogProducer: n.NewReqLogProducer(t),
		AggregateConfProducer: n.NewAggregateConfProducer(t),
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
			time.Sleep(server.GetPollTimeout())
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
	out := make([]value.Value, 1)
	err = nc.GetMulti(ctx, aggId, []value.Value{value.String("mygk")}, []value.Dict{kwargs}, out)
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
	err = nc.GetMulti(ctx, aggId, []value.Value{groupkey}, []value.Dict{kwargs}, out)
	assert.NoError(t, err)
	assert.EqualValues(t, v, out[0])
}
