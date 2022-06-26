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
	"fennel/nitrous/server"
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

	svr := server.NewServer(nil)
	ctx := context.Background()
	tierId := ftypes.RealmID(5)
	aggId := ftypes.AggId(20)

	req := &rpc.AggregateValuesRequest{
		TierId:    uint32(tierId),
		AggId:     uint32(aggId),
		Codec:     rpc.AggCodec_V1,
		Duration:  24 * 3600,
		Groupkeys: []string{"mygk"},
	}

	_, err := svr.GetAggregateValues(ctx, req)
	assert.Error(t, err)

	adm := NewAggDefsMgr(p, tlr, svr)
	err = adm.RestoreAggregates()
	assert.NoError(t, err)
	// Get fails since aggregate has not been defined yet.
	_, err = svr.GetAggregateValues(ctx, req)
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

	resp, err := svr.GetAggregateValues(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Results))
	val, err := value.FromProtoValue(resp.Results[0])
	assert.NoError(t, err)
	assert.Equal(t, value.Int(0), val)

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
	svr2 := server.NewServer(nil)
	adm = NewAggDefsMgr(p, tlr, svr2)
	err = adm.RestoreAggregates()
	assert.NoError(t, err)
	resp, err = svr2.GetAggregateValues(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Results))
	val, err = value.FromProtoValue(resp.Results[0])
	assert.NoError(t, err)
	assert.Equal(t, value.Int(42), val)
}
