package aggregate

import (
	"context"
	"testing"

	"fennel/engine/ast"
	"fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
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
			Durations: []uint64{6 * 3600, 3 * 3600},
		},
	}
	assert.NoError(t, Store(ctx, tier, agg1))
	assert.NoError(t, Store(ctx, tier, agg2))

	// now create changes
	t1 := t0 + 3600
	key := value.Nil
	keystr := key.String()

	h1 := counter.NewSum(agg1.Name, 6*3600)
	buckets := h1.BucketizeMoment(keystr, t1, value.Int(1))
	err = counter.Update(context.Background(), tier, agg1.Name, buckets, h1)
	assert.NoError(t, err)
	buckets = h1.BucketizeMoment(keystr, t1, value.Int(3))
	err = counter.Update(context.Background(), tier, agg1.Name, buckets, h1)
	assert.NoError(t, err)
	req1 := aggregate.GetAggValueRequest{AggName: "mycounter", Key: key, Kwargs: value.NewDict(map[string]value.Value{})}
	exp1 := value.Int(4)

	h2 := counter.NewMin(agg2.Name, 24*3600)
	buckets = h2.BucketizeMoment(keystr, t1, value.NewList(value.Int(2), value.Bool(false)))
	err = counter.Update(context.Background(), tier, agg2.Name, buckets, h2)
	assert.NoError(t, err)
	buckets = h2.BucketizeMoment(keystr, t1, value.NewList(value.Int(7), value.Bool(false)))
	err = counter.Update(context.Background(), tier, agg2.Name, buckets, h2)

	assert.NoError(t, err)
	req2 := aggregate.GetAggValueRequest{AggName: "minelem", Key: key, Kwargs: value.NewDict(map[string]value.Value{})}
	exp2 := value.Int(2)
	// Test kwargs with duration of an hour
	buckets = h2.BucketizeMoment(keystr, t1+5400, value.NewList(value.Int(5), value.Bool(false)))
	err = counter.Update(context.Background(), tier, agg2.Name, buckets, h2)
	assert.NoError(t, err)
	req3 := aggregate.GetAggValueRequest{AggName: "minelem", Key: key, Kwargs: value.NewDict(map[string]value.Value{"duration": value.Int(3600)})}
	exp3 := value.Int(5)

	clock.Set(int64(t1 + 2*3600))
	// Test Value()
	found1, err := Value(ctx, tier, req1.AggName, req1.Key, req1.Kwargs)
	assert.Equal(t, exp1, found1)
	found2, err := Value(ctx, tier, req2.AggName, req2.Key, req2.Kwargs)
	assert.Equal(t, exp2, found2)
	found3, err := Value(ctx, tier, req3.AggName, req3.Key, req3.Kwargs)
	assert.Equal(t, exp3, found3)
	// Test BatchValue()
	ret, err := BatchValue(ctx, tier, []aggregate.GetAggValueRequest{req1, req2, req3})
	assert.Equal(t, []value.Value{exp1, exp2, exp3}, ret)
}

// this test verifies that given a list of actions, the query is run on it to produce the right table
func TestTransformActions(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	actions := make([]action.Action, 0)
	uid := ftypes.OidType(41)
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
		assert.Equal(t, value.NewList(value.Int(uid)), get(row, "groupkey"))
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
			"name": ast.MakeString("groupkey"),
			"value": ast.List{Values: []ast.Ast{ast.Lookup{
				On:       ast.Var{Name: "var"},
				Property: "actor_id",
			}}},
		}},
	}
}

func getAction(i int, uid ftypes.OidType, ts ftypes.Timestamp, actionType ftypes.ActionType) action.Action {
	return action.Action{
		ActionID:   ftypes.OidType(1 + i),
		ActorID:    uid,
		ActorType:  "user",
		TargetID:   3,
		TargetType: "video",
		ActionType: actionType,
		Metadata:   value.Int(6),
		Timestamp:  ts,
		RequestID:  7,
	}
}
