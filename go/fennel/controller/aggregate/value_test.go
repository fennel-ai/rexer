//go:build !badger

package aggregate

import (
	"context"
	"sync"
	"testing"
	"time"

	actionlib "fennel/controller/action"
	"fennel/engine/ast"
	"fennel/kafka"
	"fennel/lib/action"
	"fennel/lib/aggregate"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/model/counter"
	_ "fennel/opdefs/std"
	_ "fennel/opdefs/std/set"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestValueAll(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	clock := &test.FakeClock{}
	tier.Clock = clock
	t0 := ftypes.Timestamp(0)
	assert.Equal(t, int64(t0), tier.Clock.Now())

	agg1 := aggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint64{6 * 3600, 3 * 3600},
		},
	}
	agg2 := aggregate.Aggregate{
		Name:      "minelem",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:   "min",
			Durations: []uint64{24 * 3600, 3 * 3600, 3600},
		},
	}
	assert.NoError(t, Store(ctx, tier, agg1))
	assert.NoError(t, Store(ctx, tier, agg2))

	agg1.Id = 1
	agg2.Id = 2

	// now create changes
	t1 := t0 + 3600
	key := value.Nil
	keystr := key.String()

	h1 := counter.NewSum(agg1.Options.Durations)
	buckets := h1.BucketizeMoment(keystr, t1, value.Int(1))
	err = counter.Update(context.Background(), tier, agg1.Id, buckets, h1)
	assert.NoError(t, err)
	buckets = h1.BucketizeMoment(keystr, t1, value.Int(3))
	err = counter.Update(context.Background(), tier, agg1.Id, buckets, h1)
	assert.NoError(t, err)
	req1 := aggregate.GetAggValueRequest{
		AggName: agg1.Name,
		Key:     key,
		Kwargs:  value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
	}
	exp1 := value.Int(4)

	h2 := counter.NewMin(agg2.Options.Durations)
	buckets = h2.BucketizeMoment(keystr, t1, value.NewList(value.Int(2), value.Bool(false)))
	err = counter.Update(context.Background(), tier, agg2.Id, buckets, h2)
	assert.NoError(t, err)
	buckets = h2.BucketizeMoment(keystr, t1, value.NewList(value.Int(7), value.Bool(false)))
	err = counter.Update(context.Background(), tier, agg2.Id, buckets, h2)

	assert.NoError(t, err)
	req2 := aggregate.GetAggValueRequest{
		AggName: agg2.Name,
		Key:     key,
		Kwargs:  value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)}),
	}
	exp2 := value.Int(2)
	// Test kwargs with duration of an hour
	buckets = h2.BucketizeMoment(keystr, t1+5400, value.NewList(value.Int(5), value.Bool(false)))
	err = counter.Update(context.Background(), tier, agg2.Id, buckets, h2)
	assert.NoError(t, err)
	req3 := aggregate.GetAggValueRequest{
		AggName: agg2.Name,
		Key:     key,
		Kwargs:  value.NewDict(map[string]value.Value{"duration": value.Int(3600)}),
	}
	exp3 := value.Int(5)

	clock.Set(int64(t1 + 2*3600))
	// Test Value()
	found1, err := Value(ctx, tier, req1.AggName, req1.Key, req1.Kwargs)
	assert.NoError(t, err)
	assert.Equal(t, exp1, found1)
	found2, err := Value(ctx, tier, req2.AggName, req2.Key, req2.Kwargs)
	assert.NoError(t, err)
	assert.Equal(t, exp2, found2)
	found3, err := Value(ctx, tier, req3.AggName, req3.Key, req3.Kwargs)
	assert.NoError(t, err)
	assert.Equal(t, exp3, found3)
	// Test BatchValue()
	ret, err := BatchValue(ctx, tier, []aggregate.GetAggValueRequest{req1, req2, req3})
	assert.NoError(t, err)
	assert.Equal(t, []value.Value{exp1, exp2, exp3}, ret)
}

func TestOfflineAggregates(t *testing.T) {
	tier, err := test.Tier()
	defer test.Teardown(tier)
	assert.NoError(t, err)
	ctx := context.Background()

	clock := test.FakeClock{}
	tier.Clock = &clock
	t1 := ftypes.Timestamp(456)
	clock.Set(int64(t1))
	t0 := ftypes.Timestamp(0)

	agg := aggregate.Aggregate{
		Name:      "agg",
		Query:     getQuery(),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:      "topk",
			Durations:    []uint64{3600},
			CronSchedule: "*/1 * * * *",
		},
		Id: 1,
	}
	assert.NoError(t, Store(ctx, tier, agg))

	a1 := getAction(1, "3434", ftypes.Timestamp(1000), "like")
	a2 := getAction(2, "123", ftypes.Timestamp(1005), "share")
	a3 := getAction(1, "325235", ftypes.Timestamp(1000), "like")
	assert.NoError(t, actionlib.BatchInsert(ctx, tier, []action.Action{a1, a2, a3}))

	// when time is not specified we use the current time to populate it
	a1.Timestamp = t1

	wg := sync.WaitGroup{}

	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
			Topic:        action.ACTIONLOG_KAFKA_TOPIC,
			GroupID:      string(agg.Name),
			OffsetPolicy: kafka.DefaultOffsetPolicy,
		})
		assert.NoError(t, err)
		defer consumer.Close()
		err = Update(ctx, tier, consumer, agg)
		assert.NoError(t, err)
	}()

	expectedJsonTable := []string{
		`{"action_id":2,"action_type":"like","actor_id":"3434","actor_type":"user","aggregate":"agg","groupkey":["3434"],"metadata":6,"request_id":"7","target_id":"3","target_type":"video","timestamp":1000}`,
		`{"action_id":2,"action_type":"like","actor_id":"325235","actor_type":"user","aggregate":"agg","groupkey":["325235"],"metadata":6,"request_id":"7","target_id":"3","target_type":"video","timestamp":1000}`,
	}
	// test that actions were written as JSON as well
	go func() {
		defer wg.Done()
		consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
			Topic:        libcounter.AGGREGATE_OFFLINE_TRANSFORM_TOPIC_NAME,
			GroupID:      utils.RandString(6),
			OffsetPolicy: kafka.DefaultOffsetPolicy,
		})
		assert.NoError(t, err)
		defer consumer.Close()
		data, err := consumer.ReadBatch(ctx, 2, 15*time.Second)
		assert.NoError(t, err)
		found := make([]string, 0, 2)
		for i := range data {
			assert.NoError(t, err)
			found = append(found, string(data[i]))
		}
		assert.Equal(t, expectedJsonTable, found)
	}()

	wg.Wait()
}

func TestCachedValueAll(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	clock := &test.FakeClock{}
	tier.Clock = clock
	t0 := ftypes.Timestamp(0)
	assert.Equal(t, int64(t0), tier.Clock.Now())
	t1 := t0 + 1800
	clock.Set(int64(t1))
	assert.Equal(t, int64(t1), tier.Clock.Now())

	agg := aggregate.Aggregate{
		Name:      "agg",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint64{3600},
		},
		Id: 1,
	}
	h := counter.NewSum(agg.Options.Durations)
	key := value.String("key")
	kwargs := value.NewDict(map[string]value.Value{"duration": value.Int(3600)})
	assert.NoError(t, Store(ctx, tier, agg))

	// initially we should get 0
	expected := value.Int(0)
	found, err := Value(ctx, tier, agg.Name, key, kwargs)
	assert.NoError(t, err)
	assert.True(t, expected.Equal(found))

	// wait for value to be cached
	time.Sleep(10 * time.Millisecond)
	// update buckets, we should still get back cached value
	buckets := h.BucketizeMoment(key.String(), t0, value.Int(1))
	assert.NoError(t, counter.Update(ctx, tier, agg.Id, buckets, h))
	expected = value.Int(0)
	found, err = Value(ctx, tier, agg.Name, key, kwargs)
	assert.NoError(t, err)
	assert.True(t, expected.Equal(found))

	// test TTL set properly
	ttl, ok := tier.PCache.GetTTL(makeCacheKey(agg.Name, key, kwargs))
	assert.True(t, ok)
	assert.LessOrEqual(t, ttl, 60*time.Second)

	// test batch now
	agg1, agg2, agg3 := agg, agg, agg
	agg1.Name, agg2.Name, agg3.Name = "agg1", "agg2", "agg3"
	reqs := []aggregate.GetAggValueRequest{
		{AggName: agg1.Name, Key: key, Kwargs: kwargs},
		{AggName: agg2.Name, Key: key, Kwargs: kwargs},
		{AggName: agg3.Name, Key: key, Kwargs: kwargs},
	}
	ids := []ftypes.AggId{2, 3, 4}
	histograms := []counter.Histogram{
		counter.NewSum(agg1.Options.Durations),
		counter.NewSum(agg2.Options.Durations),
		counter.NewSum(agg3.Options.Durations),
	}
	assert.NoError(t, Store(ctx, tier, agg1))
	assert.NoError(t, Store(ctx, tier, agg2))
	assert.NoError(t, Store(ctx, tier, agg3))

	// initially we only get req1 and req3 and we should find 0s
	reqs_ := []aggregate.GetAggValueRequest{reqs[0], reqs[2]}
	expectedVals := []value.Value{value.Int(0), value.Int(0)}
	foundVals, err := BatchValue(ctx, tier, reqs_)
	assert.NoError(t, err)
	for i, expval := range expectedVals {
		assert.True(t, expval.Equal(foundVals[i]))
	}

	// wait for values to be cached
	time.Sleep(10 * time.Millisecond)
	// update buckets, we should get back cached value from req1 and req3 but ground truth from req2
	for i, h := range histograms {
		buckets := h.BucketizeMoment(key.String(), t0, value.Int(1))
		assert.NoError(t, counter.Update(ctx, tier, ids[i], buckets, h))
	}
	expectedVals = []value.Value{value.Int(0), value.Int(1), value.Int(0)}
	foundVals, err = BatchValue(ctx, tier, reqs)
	assert.NoError(t, err)
	for i, expval := range expectedVals {
		assert.True(t, expval.Equal(foundVals[i]))
	}

	// wait for req2 value to be cached
	time.Sleep(10 * time.Millisecond)
	// test TTL set properly
	for _, req := range reqs {
		ttl, ok := tier.PCache.GetTTL(makeCacheKey(req.AggName, req.Key, req.Kwargs))
		assert.True(t, ok)
		assert.LessOrEqual(t, ttl, 60*time.Second)
	}
}

// this test verifies that given a list of actions, the query is run on it to produce the right table
func TestTransformActions(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	actions := make([]action.Action, 0)
	uid := ftypes.OidType("41")
	for i := 0; i < 100; i++ {
		// our query only looks at Like action, not share
		a1 := getAction(i, uid, ftypes.Timestamp(i+1000), "like")
		a2 := getAction(i, uid, ftypes.Timestamp(i+1005), "share")
		actions = append(actions, a1, a2)
	}

	table, err := transformActions(tier, actions, getQuery())
	assert.NoError(t, err)
	assert.Equal(t, 100, table.Len())
	for i := 0; i < table.Len(); i++ {
		r, _ := table.At(i)
		row, ok := r.(value.Dict)
		assert.True(t, ok)
		assert.Equal(t, value.Int(i+1000), get(row, "timestamp"))
		assert.Equal(t, value.NewList(value.String(uid)), get(row, "groupkey"))
	}
}

func get(d value.Dict, k string) value.Value {
	ret, _ := d.Get(k)
	return ret
}

func getQuery() ast.Ast {
	return ast.OpCall{
		Namespace: "std",
		Name:      "set",
		Operands: []ast.Ast{ast.OpCall{
			Namespace: "std",
			Name:      "filter",
			Operands:  []ast.Ast{ast.Var{Name: "actions"}},
			Vars:      []string{"e"},
			Kwargs: ast.Dict{Values: map[string]ast.Ast{
				"where": ast.Binary{
					Left:  ast.Lookup{On: ast.Var{Name: "e"}, Property: "action_type"},
					Op:    "==",
					Right: ast.MakeString("like"),
				},
			}},
		}},
		Vars: []string{"var"},
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"field": ast.MakeString("groupkey"),
			"value": ast.List{Values: []ast.Ast{ast.Lookup{
				On:       ast.Var{Name: "var"},
				Property: "actor_id",
			}}},
		}},
	}
}

func getAction(i int, uid ftypes.OidType, ts ftypes.Timestamp, actionType ftypes.ActionType) action.Action {
	return action.Action{
		ActionID:   ftypes.IDType(1 + i),
		ActorID:    uid,
		ActorType:  "user",
		TargetID:   "3",
		TargetType: "video",
		ActionType: actionType,
		Metadata:   value.Int(6),
		Timestamp:  ts,
		RequestID:  "7",
	}
}
