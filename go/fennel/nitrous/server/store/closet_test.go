package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"fennel/lib/aggregate"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server/temporal"
	"fennel/nitrous/test"

	"github.com/raulk/clock"
	"github.com/stretchr/testify/assert"
)

func TestAggregateStore(t *testing.T) {
	n := test.NewTestNitrous(t)
	opts := aggregate.Options{
		AggType:   "sum",
		Durations: []uint32{24 * 3600},
	}
	aggId := ftypes.AggId(1)
	mr, err := counter.ToMergeReduce(aggId, opts)
	assert.NoError(t, err)
	b := temporal.NewFixedWidthBucketizer(5, clock.New())
	tierId := ftypes.RealmID(1)
	cs, err := NewCloset(tierId, aggId, rpc.AggCodec_V1, mr, b, 25)
	assert.NoError(t, err)
	ctx := context.Background()
	kwargs := value.NewDict(nil)
	kwargs.Set("duration", value.Int(24*3600))

	val, err := cs.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Int(0)}, val)

	keys, vgs, err := cs.update(ctx, []uint32{uint32(time.Now().Unix())}, []string{"mygk"}, []value.Value{value.Int(5)}, n.Store)
	assert.NoError(t, err)
	err = n.Store.SetMany(ctx, keys, vgs)
	assert.NoError(t, err)
	// sleep for a bit to ensure all writes are flushed
	time.Sleep(100 * time.Millisecond)
	val, err = cs.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Int(5)}, val)

	keys, vgs, err = cs.update(ctx, []uint32{uint32(time.Now().Unix())}, []string{"mygk"}, []value.Value{value.Int(7)}, n.Store)
	assert.NoError(t, err)
	err = n.Store.SetMany(ctx, keys, vgs)
	assert.NoError(t, err)
	// sleep for a bit to ensure all writes are flushed
	time.Sleep(100 * time.Millisecond)
	val, err = cs.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Int(12)}, val)
}

func TestProcess(t *testing.T) {
	n := test.NewTestNitrous(t)
	opts := aggregate.Options{
		AggType:   "max",
		Durations: []uint32{24 * 3600},
	}
	aggId := ftypes.AggId(1)
	mr, err := counter.ToMergeReduce(aggId, opts)
	assert.NoError(t, err)
	ck := clock.NewMock()
	ck.Add(time.Since(time.Unix(0, 0)))
	b := temporal.NewFixedWidthBucketizer(100, ck)
	tierId := ftypes.RealmID(1)
	cs, err := NewCloset(tierId, aggId, rpc.AggCodec_V1, mr, b, 25)
	assert.NoError(t, err)
	ctx := context.Background()
	kwargs := value.NewDict(nil)
	kwargs.Set("duration", value.Int(24*3600))

	// function for pushing an event to Closet.
	pushEvent := func(cs *Closet, tierId ftypes.RealmID, aggId ftypes.AggId, gk string, val value.Value) {
		ev, err := value.ToProtoValue(val)
		assert.NoError(t, err)
		keys, vgs, err := cs.Process(ctx, []*rpc.NitrousOp{
			{
				TierId: uint32(tierId),
				Type:   rpc.OpType_AGG_EVENT,
				Op: &rpc.NitrousOp_AggEvent{
					AggEvent: &rpc.AggEvent{
						AggId:     uint32(aggId),
						Groupkey:  gk,
						Value:     &ev,
						Timestamp: uint32(ck.Now().Unix()),
					},
				},
			},
		}, n.Store)
		assert.NoError(t, err)
		err = n.Store.SetMany(ctx, keys, vgs)
		assert.NoError(t, err)
	}

	// max is 0.
	vals, err := cs.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Double(0)}, vals)

	// max is the inserted value.
	pushEvent(cs, tierId, aggId, "mygk", value.Int(42))
	vals, err = cs.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Int(42)}, vals)

	// max should remain as 42.
	ck.Add(10 * time.Hour)
	pushEvent(cs, tierId, aggId, "mygk", value.Int(29))
	vals, err = cs.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Int(42)}, vals)

	// Add an event for another agg in the same time window should not conflict
	// and should not overwrite.
	opts = aggregate.Options{
		AggType:   "sum",
		Durations: []uint32{24 * 3600},
	}
	aggId2 := ftypes.AggId(2)
	mr2, err := counter.ToMergeReduce(aggId2, opts)
	assert.NoError(t, err)
	cs2, err := NewCloset(tierId, aggId2, rpc.AggCodec_V1, mr2, b, 25)
	assert.NoError(t, err)
	pushEvent(cs2, tierId, aggId2, "mygk", value.Int(531))
	vals, err = cs2.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Int(531)}, vals)
	vals, err = cs.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Int(42)}, vals)

	// "42" will expire and max should now be 29 for agg1.
	// agg2 should remain unchanged.
	ck.Add(16 * time.Hour)
	pushEvent(cs, tierId, aggId, "mygk", value.Int(-10))
	vals, err = cs.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Int(29)}, vals)
	vals, err = cs2.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Int(531)}, vals)

	// "29" will expire and max should now be -10.
	// agg2 value should also expire and now return 0.
	ck.Add(10 * time.Hour)
	vals, err = cs.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Int(-10)}, vals)
	vals, err = cs2.Get(ctx, []string{"mygk"}, []value.Dict{kwargs}, n.Store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []value.Value{value.Int(0)}, vals)
}

func BenchmarkGet(b *testing.B) {
	n := test.NewTestNitrous(b)
	opts := aggregate.Options{
		AggType:   "max",
		Durations: []uint32{24 * 3600},
	}
	aggId := ftypes.AggId(1)
	mr, err := counter.ToMergeReduce(aggId, opts)
	assert.NoError(b, err)
	ck := clock.NewMock()
	ck.Add(time.Since(time.Unix(0, 0)))
	bucketizer := temporal.NewFixedWidthBucketizer(100, ck)
	tierId := ftypes.RealmID(1)
	cs, err := NewCloset(tierId, aggId, rpc.AggCodec_V1, mr, bucketizer, 25)
	assert.NoError(b, err)

	ctx := context.Background()

	var gks []string
	var vals []value.Value
	var kwargs []value.Dict
	var ts []uint32
	now := uint32(n.Clock.Now().Unix())
	duration := value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)})
	for i := 0; i < 1000; i++ {
		gks = append(gks, fmt.Sprintf("mygk-%d", i))
		vals = append(vals, value.Int(i))
		ts = append(ts, now)
		kwargs = append(kwargs, duration)
	}
	keys, vgs, err := cs.update(ctx, ts, gks, vals, n.Store)
	assert.NoError(b, err)
	err = n.Store.SetMany(ctx, keys, vgs)
	assert.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := cs.Get(ctx, gks, kwargs, n.Store)
		assert.NoError(b, err)
	}
}

func TestKeyGroupsToRead(t *testing.T) {
	opts := aggregate.Options{
		AggType:   "max",
		Durations: []uint32{24 * 3600},
	}
	aggId := ftypes.AggId(1)
	mr, err := counter.ToMergeReduce(aggId, opts)
	assert.NoError(t, err)

	ck := clock.NewMock()
	ck.Add(time.Since(time.Unix(0, 0)))
	bucketizer := temporal.NewFixedWidthBucketizer(100, ck)

	tierId := ftypes.RealmID(1)
	cs, err := NewCloset(tierId, aggId, rpc.AggCodec_V1, mr, bucketizer, 25)
	assert.NoError(t, err)

	timeRange := temporal.TimeBucketRange{
		Width: 100,
	}

	// Start and End index in different first-level buckets.
	timeRange.StartIdx = 24
	timeRange.EndIdx = 25
	kgs, err := cs.getKeyGroupsToRead("mygk", timeRange)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(kgs))

	// Start and End index in the same first-level bucket.
	timeRange.StartIdx = 23
	timeRange.EndIdx = 24
	kgs, err = cs.getKeyGroupsToRead("mygk", timeRange)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(kgs))

	// Start and End index that enclose 3 full first-level buckets.
	timeRange.StartIdx = 13
	timeRange.EndIdx = 4*25 + 9
	kgs, err = cs.getKeyGroupsToRead("mygk", timeRange)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(kgs))

	// Start idx aligned with start of second-level buckets under key.
	timeRange.StartIdx = 25
	timeRange.EndIdx = 3*25 + 10
	kgs, err = cs.getKeyGroupsToRead("mygk", timeRange)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(kgs))

	// End idx aligned with end of second-level buckets under key.
	timeRange.StartIdx = 13
	timeRange.EndIdx = 3*25 - 1
	kgs, err = cs.getKeyGroupsToRead("mygk", timeRange)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(kgs))
}

func TestKeyGroupsToUpdate(t *testing.T) {
	opts := aggregate.Options{
		AggType:   "max",
		Durations: []uint32{24 * 3600},
	}
	aggId := ftypes.AggId(1)
	mr, err := counter.ToMergeReduce(aggId, opts)
	assert.NoError(t, err)

	ck := clock.NewMock()
	ck.Add(time.Since(time.Unix(0, 0)))
	bucketizer := temporal.NewFixedWidthBucketizer(100, ck)

	tierId := ftypes.RealmID(1)
	cs, err := NewCloset(tierId, aggId, rpc.AggCodec_V1, mr, bucketizer, 25)
	assert.NoError(t, err)

	buckets := []temporal.TimeBucket{
		{
			Width: 100,
			Index: 25,
		},
		{
			Width: 50,
			Index: 50,
		},
	}
	kgs, err := cs.getKeyGroupsToUpdate("mygk", buckets)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(kgs))
	for _, kg := range kgs {
		assert.Equal(t, 2, len(kg.Fields.MustGet()))
	}
}
