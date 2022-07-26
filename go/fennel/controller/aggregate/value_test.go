package aggregate

import (
	"context"
	"fmt"
	"testing"
	"time"

	agg_test "fennel/controller/aggregate/test"
	"fennel/engine/ast"
	"fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	_ "fennel/opdefs/std"
	_ "fennel/opdefs/std/set"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestValueAll(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	ctx := context.Background()
	clock := &test.FakeClock{}
	tier.Clock = clock
	t0 := ftypes.Timestamp(0)
	assert.Equal(t, uint32(t0), tier.Clock.Now())

	agg1 := aggregate.Aggregate{
		Id:        1,
		Name:      "mycounter",
		Query:     agg_test.GetDummyAggQuery(),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint32{6 * 3600, 3 * 3600, 48 * 3600},
		},
	}
	agg2 := aggregate.Aggregate{
		Id:        2,
		Name:      "minelem",
		Query:     agg_test.GetDummyAggQuery(),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:   "min",
			Durations: []uint32{24 * 3600, 3 * 3600, 3600},
		},
	}
	assert.NoError(t, Store(ctx, tier, agg1))
	assert.NoError(t, Store(ctx, tier, agg2))

	// now create changes
	t1 := t0 + 3600
	key := value.Nil

	actions := []action.Action{
		{
			ActorID:   "5",
			TargetID:  "7",
			RequestID: "1234",
			Metadata: value.NewDict(map[string]value.Value{
				"groupkey":  key,
				"value":     value.Int(4),
				"timestamp": value.Int(t1),
			}),
		},
	}
	err := Update(ctx, tier, actions, agg1)
	assert.NoError(t, err)
	req1 := aggregate.GetAggValueRequest{
		AggName: agg1.Name,
		Key:     key,
		Kwargs:  value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
	}
	exp1 := value.Int(4)

	actions = []action.Action{
		{
			ActorID:   "5",
			TargetID:  "7",
			RequestID: "1234",
			Metadata: value.NewDict(map[string]value.Value{
				"groupkey":  key,
				"value":     value.Int(2),
				"timestamp": value.Int(t1),
			}),
		},
		{
			ActorID:   "5",
			TargetID:  "7",
			RequestID: "1234",
			Metadata: value.NewDict(map[string]value.Value{
				"groupkey":  key,
				"value":     value.Int(7),
				"timestamp": value.Int(t1),
			}),
		},
	}
	err = Update(ctx, tier, actions, agg2)
	assert.NoError(t, err)
	req2 := aggregate.GetAggValueRequest{
		AggName: agg2.Name,
		Key:     key,
		Kwargs:  value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)}),
	}
	exp2 := value.Int(2)

	// Test kwargs with duration of an hour
	actions = []action.Action{
		{
			ActorID:   "5",
			TargetID:  "7",
			RequestID: "1234",
			Metadata: value.NewDict(map[string]value.Value{
				"groupkey":  key,
				"value":     value.Int(5),
				"timestamp": value.Int(t1 + 5400),
			}),
		},
	}
	err = Update(ctx, tier, actions, agg2)
	assert.NoError(t, err)
	req3 := aggregate.GetAggValueRequest{
		AggName: agg2.Name,
		Key:     key,
		Kwargs:  value.NewDict(map[string]value.Value{"duration": value.Int(3600)}),
	}
	exp3 := value.Int(5)

	clock.Set(uint32(t1 + 48*3600))

	req4 := aggregate.GetAggValueRequest{
		AggName: agg1.Name,
		Key:     key,
		Kwargs:  value.NewDict(map[string]value.Value{"duration": value.Int(48 * 3600)}),
	}
	found4, err := Value(ctx, tier, req4.AggName, req4.Key, req4.Kwargs)
	assert.NoError(t, err)
	assert.Equal(t, value.Int(4), found4)

	clock.Set(uint32(t1 + 2*3600))
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

func TestCachedValueAll(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	ctx := context.Background()
	clock := &test.FakeClock{}
	tier.Clock = clock
	t0 := ftypes.Timestamp(0)
	assert.Equal(t, uint32(t0), tier.Clock.Now())
	t1 := t0 + 1800
	clock.Set(uint32(t1))
	assert.Equal(t, uint32(t1), tier.Clock.Now())

	agg := aggregate.Aggregate{
		Id:        1,
		Name:      "agg",
		Query:     agg_test.GetDummyAggQuery(),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint32{3600},
		},
	}
	assert.NoError(t, Store(ctx, tier, agg))

	// initially we should get 0
	key := value.String("key")
	kwargs := value.NewDict(map[string]value.Value{"duration": value.Int(3600)})
	found, err := Value(ctx, tier, agg.Name, key, kwargs)
	assert.NoError(t, err)
	expected := value.Int(0)
	assert.True(t, expected.Equal(found))

	// wait for value to be cached
	time.Sleep(10 * time.Millisecond)
	// update buckets, we should still get back cached value
	actions := []action.Action{
		{
			ActorID:   "5",
			TargetID:  "7",
			RequestID: "1234",
			Metadata: value.NewDict(map[string]value.Value{
				"groupkey":  key,
				"value":     value.Int(1),
				"timestamp": value.Int(t0),
			}),
		},
	}
	assert.NoError(t, Update(ctx, tier, actions, agg))
	expected = value.Int(0)
	found, err = Value(ctx, tier, agg.Name, key, kwargs)
	assert.NoError(t, err)
	assert.True(t, expected.Equal(found))

	// test TTL set properly
	ttl, ok := tier.PCache.GetTTL(makeCacheKey(agg.Name, key, kwargs))
	assert.True(t, ok)
	assert.LessOrEqual(t, ttl, cacheValueDuration)

	// test batch now
	agg1, agg2, agg3 := agg, agg, agg
	agg1.Name, agg2.Name, agg3.Name = "agg1", "agg2", "agg3"
	agg1.Id, agg2.Id, agg3.Id = 2, 3, 4
	assert.NoError(t, Store(ctx, tier, agg1))
	assert.NoError(t, Store(ctx, tier, agg2))
	assert.NoError(t, Store(ctx, tier, agg3))

	// initially we only get req1 and req3 and we should find 0s
	reqs := []aggregate.GetAggValueRequest{
		{AggName: agg1.Name, Key: key, Kwargs: kwargs},
		{AggName: agg2.Name, Key: key, Kwargs: kwargs},
		{AggName: agg3.Name, Key: key, Kwargs: kwargs},
	}
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
	for _, agg := range []aggregate.Aggregate{agg1, agg2, agg3} {
		assert.NoError(t, Update(ctx, tier, actions, agg))
	}
	// and this works even with repeated requests
	req2 := []aggregate.GetAggValueRequest{reqs[0], reqs[2], reqs[1], reqs[1], reqs[2]}
	expectedVals = []value.Value{value.Int(0), value.Int(0), value.Int(1), value.Int(1), value.Int(0)}
	// expectedVals = []value.Value{value.Int(0), value.Int(1), value.Int(0)}
	foundVals, err = BatchValue(ctx, tier, req2)
	assert.NoError(t, err)
	for i, expval := range expectedVals {
		assert.True(t, expval.Equal(foundVals[i]), fmt.Sprintf("%d: %s != %s", i, expval, foundVals[i]))
	}

	// wait for req2 value to be cached
	time.Sleep(10 * time.Millisecond)
	// test TTL set properly
	for _, req := range reqs {
		ttl, ok := tier.PCache.GetTTL(makeCacheKey(req.AggName, req.Key, req.Kwargs))
		assert.True(t, ok)
		assert.LessOrEqual(t, ttl, cacheValueDuration)
	}
}

// this test verifies that given a list of actions, the query is run on it to produce the right table
func TestTransformActions(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	actions := make([]action.Action, 0)
	uid := ftypes.OidType("41")
	for i := 0; i < 100; i++ {
		// our query only looks at Like action, not share
		a1 := getAction(i, uid, ftypes.Timestamp(i+1000), "like")
		a2 := getAction(i, uid, ftypes.Timestamp(i+1005), "share")
		actions = append(actions, a1, a2)
	}

	table, err := Transform(tier, actions, getQuery())
	assert.NoError(t, err)
	assert.Equal(t, 100, table.Len())
	for i := 0; i < table.Len(); i++ {
		r, _ := table.At(i)
		row, ok := r.(value.Dict)
		assert.True(t, ok)
		assert.Equal(t, value.Int(i+1000), get(row, "timestamp"))
		assert.Equal(t, value.NewList(value.Int(41)), get(row, "groupkey"))
	}
}

// this test verifies that given a list of actions, the query is run on it to produce the right table
func TestTransformValues(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	values := make([]value.Value, 0, 2)
	for i := 0; i < 2; i++ {
		// our query only looks at Like action, not share
		values = append(values, value.NewDict(map[string]value.Value{
			"user_id":     value.Int(i + 123),
			"stream_name": value.String("conn1"),
		}))
		values = append(values, value.NewDict(map[string]value.Value{
			"user_id":     value.Int(i + 11000),
			"stream_name": value.String("conn2"),
		}))
	}
	table, err := Transform(tier, values, getValueQuery())
	assert.NoError(t, err)
	assert.Equal(t, 2, table.Len())
	for i := 0; i < table.Len(); i++ {
		r, _ := table.At(i)
		row, ok := r.(value.Dict)
		assert.True(t, ok)
		assert.Equal(t, value.Int(i+123), get(row, "actor_id"))
		assert.Equal(t, value.String("user"), get(row, "actor_type"))
	}
}

func get(d value.Dict, k string) value.Value {
	ret, _ := d.Get(k)
	return ret
}

func getQuery() ast.Ast {
	return &ast.OpCall{
		Namespace: "std",
		Name:      "set",
		Operands: []ast.Ast{&ast.OpCall{
			Namespace: "std",
			Name:      "filter",
			Operands:  []ast.Ast{&ast.Var{Name: "actions"}},
			Vars:      []string{"e"},
			Kwargs: ast.MakeDict(map[string]ast.Ast{
				"where": &ast.Binary{
					Left:  &ast.Lookup{On: &ast.Var{Name: "e"}, Property: "action_type"},
					Op:    "==",
					Right: ast.MakeString("like"),
				},
			}),
		}},
		Vars: []string{"var"},
		Kwargs: ast.MakeDict(map[string]ast.Ast{
			"field": ast.MakeString("groupkey"),
			"value": &ast.List{Values: []ast.Ast{&ast.Lookup{
				On:       &ast.Var{Name: "var"},
				Property: "actor_id",
			}}},
		}),
	}
}

func getValueQuery() ast.Ast {
	return &ast.OpCall{
		Namespace: "std",
		Name:      "set",
		Operands: []ast.Ast{&ast.OpCall{
			Namespace: "std",
			Name:      "set",
			Operands: []ast.Ast{&ast.OpCall{
				Namespace: "std",
				Name:      "filter",
				Operands:  []ast.Ast{&ast.Var{Name: "stream"}},
				Vars:      []string{"s"},
				Kwargs: ast.MakeDict(map[string]ast.Ast{
					"where": &ast.Binary{
						Left:  &ast.Lookup{On: &ast.Var{Name: "s"}, Property: "stream_name"},
						Op:    "==",
						Right: ast.MakeString("conn1"),
					},
				}),
			}},
			Vars: []string{"e"},
			Kwargs: ast.MakeDict(map[string]ast.Ast{
				"field": ast.MakeString("actor_id"),
				"value": &ast.Lookup{
					On:       &ast.Var{Name: "e"},
					Property: "user_id",
				},
			}),
		}},
		Kwargs: ast.MakeDict(map[string]ast.Ast{
			"field": ast.MakeString("actor_type"),
			"value": ast.MakeString("user"),
		}),
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
