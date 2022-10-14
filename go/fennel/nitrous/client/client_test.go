package client_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/nitrous/client"
	"fennel/nitrous/rpc"
	"fennel/nitrous/test"
	"fennel/test/nitrous"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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
			time.Sleep(server.GetBinlogPollTimeout())
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

func TestPushWithDifferentMarshalingCode(t *testing.T) {
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
			time.Sleep(server.GetBinlogPollTimeout())
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

	// Push events for the aggregate.

	// serialized without vitess - this is to simulate that an old message in the binlog prior to using vitess marshaling
	// code
	kwargs := value.NewDict(nil)
	kwargs.Set("duration", value.Int(24*3600))
	groupkey1 := value.String("mygk")
	v1 := value.Int(124)
	{
		pv, err := value.ToProtoValue(v1)
		assert.NoError(t, err)
		// serialize using proto marshal
		op := rpc.NitrousOpFromVTPool()
		op.TierId = uint32(nc.ID())
		op.Type = rpc.OpType_AGG_EVENT
		op.Op = &rpc.NitrousOp_AggEvent{
			AggEvent: &rpc.AggEvent{
				AggId:     uint32(aggId),
				Groupkey:  groupkey1.String(),
				Value:     &pv,
				Timestamp: uint32(time.Now().Unix()),
			},
		}
		dproto, err := proto.Marshal(op)
		assert.NoError(t, err)
		err = cfg.BinlogProducer.Log(ctx, dproto, nil)
		assert.NoError(t, err)
	}

	// serialized with vitess - simulates recent code using vitess marshaling code
	groupkey2 := value.String("mygk2")
	v2 := value.Int(125)
	{
		pv, err := value.ToProtoValue(v2)
		assert.NoError(t, err)
		// serialize using proto marshal
		op := rpc.NitrousOpFromVTPool()
		op.TierId = uint32(nc.ID())
		op.Type = rpc.OpType_AGG_EVENT
		op.Op = &rpc.NitrousOp_AggEvent{
			AggEvent: &rpc.AggEvent{
				AggId:     uint32(aggId),
				Groupkey:  groupkey2.String(),
				Value:     &pv,
				Timestamp: uint32(time.Now().Unix()),
			},
		}
		dproto, err := op.MarshalVT()
		assert.NoError(t, err)
		err = cfg.BinlogProducer.Log(ctx, dproto, nil)
		assert.NoError(t, err)
	}

	// Wait for the event to be consumed.
	waitToConsume()
	out := make([]value.Value, 2)
	err = nc.GetMulti(ctx, aggId, []value.Value{groupkey1, groupkey2}, []value.Dict{kwargs, kwargs}, out)
	assert.NoError(t, err)

	assert.EqualValues(t, v1, out[0])
	assert.EqualValues(t, v2, out[1])
}

func TestSerializationProto(t *testing.T) {
	// serialized without vitess
	v := value.Int(1)
	pv, err := value.ToProtoValue(v)
	assert.NoError(t, err)
	op := rpc.NitrousOp{
		TierId: uint32(1),
		Type: rpc.OpType_AGG_EVENT,
		Op: &rpc.NitrousOp_AggEvent{
			AggEvent: &rpc.AggEvent{
				AggId: uint32(2),
				Groupkey: utils.RandString(5),
				Timestamp: uint32(10000),
				Value: &pv,
			},
		},
	}
	fmt.Printf("op: %v\n", &op)
	rawop, err := proto.Marshal(&op)
	assert.NoError(t, err)

	// serialized with vitess
	actualOp := rpc.NitrousOpFromVTPool()
	err = actualOp.UnmarshalVT(rawop)
	assert.NoError(t, err)
	fmt.Printf("actualOp: %v\n", actualOp)
	assert.True(t, actualOp.EqualVT(&op))

	protoActualOp := rpc.NitrousOp{}
	err = proto.Unmarshal(rawop, &protoActualOp)
	assert.NoError(t, err)
	fmt.Printf("protoActualOp: %v\n", &protoActualOp)
	assert.Equal(t, protoActualOp.String(), op.String())
}

func TestSerializationVitessProto(t *testing.T) {
	// serialized without vitess
	v := value.Int(1)
	pv, err := value.ToProtoValue(v)
	assert.NoError(t, err)
	op := rpc.NitrousOpFromVTPool()
	op.TierId = uint32(1)
	op.Type = rpc.OpType_AGG_EVENT
	op.Op = &rpc.NitrousOp_AggEvent{
		AggEvent: &rpc.AggEvent{
			AggId: uint32(2),
			Groupkey: utils.RandString(5),
			Timestamp: uint32(10000),
			Value: &pv,
		},
	}
	fmt.Printf("op: %v\n", op)
	rawop, err := op.MarshalVT()
	assert.NoError(t, err)

	// serialized with vitess
	actualOp := rpc.NitrousOpFromVTPool()
	err = actualOp.UnmarshalVT(rawop)
	assert.NoError(t, err)
	fmt.Printf("actualOp: %v\n", actualOp)
	assert.True(t, actualOp.EqualVT(op))

	protoActualOp := rpc.NitrousOp{}
	err = proto.Unmarshal(rawop, &protoActualOp)
	assert.NoError(t, err)
	fmt.Printf("protoActualOp: %v\n", &protoActualOp)
	assert.Equal(t, protoActualOp.String(), op.String())
}

func TestSerializationVitessProtoTesting(t *testing.T) {
	// serialized without vitess
	v := value.Nil
	pv, err := value.ToProtoValue(v)
	assert.NoError(t, err)
	op := rpc.NitrousOpFromVTPool()
	op.TierId = uint32(1)
	op.Type = rpc.OpType_AGG_EVENT
	op.Op = &rpc.NitrousOp_AggEvent{
		AggEvent: &rpc.AggEvent{
			AggId: uint32(2),
			Groupkey: utils.RandString(5),
			Timestamp: uint32(10000),
			Value: &pv,
		},
	}
	rawop, err := op.MarshalVT()
	assert.NoError(t, err)

	// serialized with vitess
	actualOp := rpc.NitrousOpFromVTPool()
	err = actualOp.UnmarshalVT(rawop)
	assert.NoError(t, err)

	// TODO(mohit): remove this, https://github.com/planetscale/vtprotobuf/issues/60
	// assert.True(t, actualOp.EqualVT(op))

	val, err := value.FromProtoValue(actualOp.GetAggEvent().Value)
	assert.NoError(t, err)
	assert.True(t, val.Equal(value.Nil))

	protoActualOp := rpc.NitrousOp{}
	err = proto.Unmarshal(rawop, &protoActualOp)
	assert.NoError(t, err)

	// TODO(mohit): remove this, https://github.com/planetscale/vtprotobuf/issues/60
	// assert.Equal(t, protoActualOp.String(), op.String())
}