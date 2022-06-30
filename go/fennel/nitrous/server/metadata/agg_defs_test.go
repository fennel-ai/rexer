package metadata

import (
	"context"
	"testing"
	"time"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/nitrous"
	"fennel/lib/value"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server/tailer"
	"fennel/plane"

	"github.com/stretchr/testify/assert"
)

func TestInitRestore(t *testing.T) {
	tp := plane.NewTestPlane(t)
	p := tp.Plane
	// Create producer so the topic is initialized.
	producer := tp.NewProducer(t, nitrous.BINLOG_KAFKA_TOPIC)

	tlr := tailer.NewTestTailer(p, nitrous.BINLOG_KAFKA_TOPIC)
	// Start tailing and wait for the consumer to be assigned partitions.
	// Before the consumer is assigned partitions, it is not possible to measure
	// the lag.
	go tlr.Tail()
	for {
		offs, err := tlr.GetOffsets()
		assert.NoError(t, err)
		if len(offs) > 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	ctx := context.Background()
	tierId := ftypes.RealmID(5)
	aggId := ftypes.AggId(20)

	adm := NewAggDefsMgr(p, tlr)
	// Get fails since aggregate has not been defined yet.
	_, err := adm.Get(ctx, tierId, aggId, rpc.AggCodec_V1, 24*3600, []string{"mygk"})
	assert.Error(t, err)
	err = adm.RestoreAggregates()
	assert.NoError(t, err)
	_, err = adm.Get(ctx, tierId, aggId, rpc.AggCodec_V1, 24*3600, []string{"mygk"})
	assert.Error(t, err)

	// Define the aggregate.
	ks, vgs, err := adm.Process(ctx, []*rpc.NitrousOp{
		{
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
		},
	})
	assert.NoError(t, err)
	err = p.Store.SetMany(ks, vgs)
	assert.NoError(t, err)

	resp, err := adm.Get(ctx, tierId, aggId, rpc.AggCodec_V1, 24*3600, []string{"mygk"})
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
	// Wait for the event to be processed.
	for {
		lag, err := tlr.GetLag()
		assert.NoError(t, err)
		if lag == 0 {
			// Sleep a brief amount of time to allow the read from consumer too
			// actually be processed.
			time.Sleep(5 * time.Second)
			break
		} else {
			time.Sleep(tlr.GetPollTimeout())
		}
	}

	// Restore on a new "server".
	adm = NewAggDefsMgr(p, tlr)
	err = adm.RestoreAggregates()
	assert.NoError(t, err)
	resp, err = adm.Get(ctx, tierId, aggId, rpc.AggCodec_V1, 24*3600, []string{"mygk"})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(42), resp[0])
}

func TestRegisterDuplicate(t *testing.T) {
	tp := plane.NewTestPlane(t)
	p := tp.Plane
	tlr := tailer.NewTestTailer(p, nitrous.BINLOG_KAFKA_TOPIC)
	adm := NewAggDefsMgr(p, tlr)
	tierId := ftypes.RealmID(1)
	aggId := ftypes.AggId(1)
	codec := rpc.AggCodec_V1
	err := adm.registerHandler(aggKey{tierId, aggId, codec}, nil)
	assert.NoError(t, err)
	err = adm.registerHandler(aggKey{tierId, aggId, codec}, nil)
	assert.Error(t, err)
}
