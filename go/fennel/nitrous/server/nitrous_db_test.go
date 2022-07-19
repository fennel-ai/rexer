package server

import (
	"context"
	"testing"
	"time"

	"fennel/hangar"
	"fennel/kafka"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/nitrous/rpc"
	"fennel/nitrous/test"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestInitRestore(t *testing.T) {
	n := test.NewTestNitrous(t)
	adm, err := InitDB(n.Nitrous)
	assert.NoError(t, err)

	ctx := context.Background()
	tierId := ftypes.RealmID(5)
	aggId := ftypes.AggId(20)
	kwargs := value.NewDict(nil)
	kwargs.Set("duration", value.Int(24*3600))

	adm.SetPollTimeout(100 * time.Millisecond)
	adm.Start()
	defer adm.Stop()
	wait := func() {
		count := 0
		for count < 3 {
			time.Sleep(adm.tailer.GetPollTimeout())
			lag, err := adm.GetLag(ctx)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			assert.NoError(t, err)
			n.Logger.Info("Current lag", zap.Int("value", lag))
			if lag == 0 {
				count++
			}
		}
		// It is possible for the lag to be zero but the event to not have
		// been processed yet. Sleep some more to reduce the likelihood of
		// that happening.
		time.Sleep(1 * time.Second)
	}

	// Before the aggregate is created, we should get an error on trying to
	// read its value.
	_, err = adm.Get(ctx, tierId, aggId, rpc.AggCodec_V1, []string{"mygk"}, []value.Dict{kwargs})
	assert.Error(t, err)

	// Define the aggregate.
	producer := n.NewBinlogProducer(t)
	err = producer.LogProto(ctx, &rpc.NitrousOp{
		TierId: uint32(tierId),
		Type:   rpc.OpType_CREATE_AGGREGATE,
		Op: &rpc.NitrousOp_CreateAggregate{
			CreateAggregate: &rpc.CreateAggregate{
				AggId: uint32(aggId),
				Options: &aggregate.AggOptions{
					AggType:   "sum",
					Durations: []uint32{24 * 3600},
				},
			},
		},
	}, nil)
	assert.NoError(t, err)
	err = producer.Flush(5 * time.Second)
	assert.NoError(t, err)
	wait()

	// After the aggregate is created, we should get the zero value before any
	// event is logged  for the aggregate.
	resp, err := adm.Get(ctx, tierId, aggId, rpc.AggCodec_V1, []string{"mygk"}, []value.Dict{kwargs})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp))
	assert.Equal(t, value.Int(0), resp[0])

	// Create some aggregate events.
	ev, err := value.ToProtoValue(value.Int(42))
	assert.NoError(t, err)
	op := &rpc.NitrousOp{
		TierId: uint32(tierId),
		Type:   rpc.OpType_AGG_EVENT,
		Op: &rpc.NitrousOp_AggEvent{
			AggEvent: &rpc.AggEvent{
				AggId:     uint32(aggId),
				Groupkey:  "mygk",
				Value:     &ev,
				Timestamp: uint32(time.Now().Unix()),
			},
		},
	}
	err = producer.LogProto(ctx, op, nil)
	assert.NoError(t, err)
	err = producer.Flush(5 * time.Second)
	assert.NoError(t, err)
	wait()

	// Read the aggregate value - it should be 42.
	resp, err = adm.Get(ctx, tierId, aggId, rpc.AggCodec_V1, []string{"mygk"}, []value.Dict{kwargs})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(42), resp[0])

	// Restore on a new instance of the same store and read the same value.
	adm, err = InitDB(n.Nitrous)
	assert.NoError(t, err)
	resp, err = adm.Get(ctx, tierId, aggId, rpc.AggCodec_V1, []string{"mygk"}, []value.Dict{kwargs})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(42), resp[0])
}

func TestCreateDuplicate(t *testing.T) {
	n := test.NewTestNitrous(t)
	adm, err := InitDB(n.Nitrous)
	assert.NoError(t, err)
	op := &rpc.CreateAggregate{
		AggId: 1,
		Options: &aggregate.AggOptions{
			AggType:   "sum",
			Durations: []uint32{24 * 3600},
		},
	}
	tierId := ftypes.RealmID(5)
	vg := hangar.ValGroup{}
	vg, err = adm.processCreateEvent(tierId, op, vg)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vg.Fields))
	// This should be a no-op and therefore we should get no errors.
	vg, err = adm.processCreateEvent(tierId, op, vg)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vg.Fields))
	// This should fail.
	op.Options.AggType = "max"
	vg, err = adm.processCreateEvent(tierId, op, vg)
	assert.Error(t, err)
	assert.Equal(t, 1, len(vg.Fields))
}

func TestDeleteAggregate(t *testing.T) {
	n := test.NewTestNitrous(t)
	adm, err := InitDB(n.Nitrous)
	assert.NoError(t, err)
	tierId := ftypes.RealmID(5)
	ctx := context.Background()
	kwargs := value.NewDict(nil)
	kwargs.Set("duration", value.Int(24*3600))

	// Create an aggregate.
	op := &rpc.CreateAggregate{
		AggId: 1,
		Options: &aggregate.AggOptions{
			AggType:   "sum",
			Durations: []uint32{24 * 3600},
		},
	}
	vg := hangar.ValGroup{}
	vg, err = adm.processCreateEvent(tierId, op, vg)
	assert.NoError(t, err)

	// Fetch aggregate value. This should return the zero value for the aggregate.
	vals, err := adm.Get(ctx, tierId, 1, rpc.AggCodec_V1, []string{"mygk"}, []value.Dict{kwargs})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vals))
	assert.EqualValues(t, 0, vals[0].(value.Int))

	// Now, delete the aggregate.
	del := &rpc.DeleteAggregate{
		AggId: 1,
	}
	_, err = adm.processDeleteEvent(tierId, del, vg) //nolint:ineffassign
	assert.NoError(t, err)

	// Fetching the aggregate should now fail.
	_, err = adm.Get(ctx, tierId, 1, rpc.AggCodec_V1, []string{"mygk"}, []value.Dict{kwargs})
	assert.Error(t, err)
}

func TestGetLag(t *testing.T) {
	n := test.NewTestNitrous(t)
	adm, err := InitDB(n.Nitrous)
	assert.NoError(t, err)

	// Produce a message for tailer.
	producer := n.NewBinlogProducer(t)

	// Set a very long test timeout so message is not really consumed and then
	// restart tailer.
	adm.tailer.SetPollTimeout(1 * time.Minute)
	adm.Start()

	ctx := context.Background()

	// Initial lag should be 0. It's possible we get an error if the consumer
	// hasn't yet been assigned a partition by the broker.
	for {
		lag, err := adm.GetLag(ctx)
		if err == nil {
			assert.EqualValues(t, 0, lag)
			break
		}
		assert.ErrorIs(t, err, kafka.ErrNoPartition)
		time.Sleep(1 * time.Second)
	}

	err = producer.Log(ctx, []byte("hello world"), nil)
	assert.NoError(t, err)
	err = producer.Flush(10 * time.Second)
	assert.NoError(t, err)

	// Lag should now be 1.
	time.Sleep(5 * time.Second)
	lag, err := adm.GetLag(ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, lag)
}
