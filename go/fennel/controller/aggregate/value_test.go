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
			AggType:  "count",
			Duration: 6 * 3600,
		},
	}
	agg2 := aggregate.Aggregate{
		Name:      "minelem",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:  "min",
			Duration: 6 * 3600,
		},
	}
	assert.NoError(t, Store(ctx, tier, agg1))
	assert.NoError(t, Store(ctx, tier, agg2))

	// now create changes
	t1 := t0 + 3600
	key := value.Nil
	keystr := key.String()

	h1 := counter.RollingCounter{Duration: 6 * 3600}
	buckets := counter.BucketizeMoment(keystr, t1, value.Int(1), h1.Windows())
	err = counter.Update(context.Background(), tier, agg1.Name, buckets, h1)
	assert.NoError(t, err)
	buckets = counter.BucketizeMoment(keystr, t1, value.Int(3), h1.Windows())
	err = counter.Update(context.Background(), tier, agg1.Name, buckets, h1)
	assert.NoError(t, err)
	req1 := aggregate.GetAggValueRequest{AggName: "mycounter", Key: key}
	exp1 := value.Int(4)

	h2 := counter.Min{Duration: 6 * 3600}
	buckets = counter.BucketizeMoment(keystr, t1, value.List{value.Int(2), value.Bool(false)}, h2.Windows())
	err = counter.Update(context.Background(), tier, agg2.Name, buckets, h2)
	assert.NoError(t, err)
	buckets = counter.BucketizeMoment(keystr, t1, value.List{value.Int(7), value.Bool(false)}, h2.Windows())
	err = counter.Update(context.Background(), tier, agg2.Name, buckets, h2)
	assert.NoError(t, err)
	req2 := aggregate.GetAggValueRequest{AggName: "minelem", Key: key}
	exp2 := value.Int(2)

	clock.Set(int64(t1 + 60))
	// Test Value()
	found1, err := Value(ctx, tier, req1.AggName, req1.Key)
	assert.Equal(t, found1, exp1)
	found2, err := Value(ctx, tier, req2.AggName, req2.Key)
	assert.Equal(t, found2, exp2)
	// Test BatchValue()
	ret, err := BatchValue(ctx, tier, []aggregate.GetAggValueRequest{req1, req2})
	assert.Equal(t, []value.Value{exp1, exp2}, ret)
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
	for i, row := range table.Pull() {
		assert.Equal(t, value.Int(i+1000), row["timestamp"])
		assert.Equal(t, value.List{value.Int(uid)}, row["groupkey"])
	}
}

func getQuery() ast.Ast {
	return ast.OpCall{
		Operand: ast.OpCall{
			Operand:   ast.Lookup{On: ast.Var{Name: "args"}, Property: "actions"},
			Namespace: "std",
			Name:      "filter",
			Kwargs: ast.Dict{Values: map[string]ast.Ast{
				"where": ast.Binary{
					Left:  ast.Lookup{On: ast.At{}, Property: "action_type"},
					Op:    "==",
					Right: ast.MakeString("like"),
				},
			}},
		},
		Namespace: "std",
		Name:      "addField",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"name": ast.MakeString("groupkey"),
			"value": ast.List{Values: []ast.Ast{ast.Lookup{
				On:       ast.At{},
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
