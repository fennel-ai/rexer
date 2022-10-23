package aggregate

import (
	"context"
	"testing"
	"time"

	clock2 "github.com/raulk/clock"
	"github.com/stretchr/testify/assert"

	controller_action "fennel/controller/action"
	"fennel/controller/aggregate"
	"fennel/engine/ast"
	"fennel/lib/action"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	_ "fennel/opdefs/std/set"
	"fennel/test"
	"fennel/test/nitrous"
	"fennel/test/optest"
)

func TestAggValue_Apply(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	clock := tier.Clock.(*clock2.Mock)

	ctx := context.Background()

	// create an aggregate, store it, create some actions, update aggregate and ensure its value returns some result
	agg := libaggregate.Aggregate{
		Name: "counter1", Query: getQuery(), Timestamp: 123,
		Source: libaggregate.SOURCE_ACTION,
		Mode:   "rql",
		Options: libaggregate.Options{
			AggType:   "sum",
			Durations: []uint32{6 * 3600, 3 * 3600, 2000},
		},
		Id: 1,
	}
	agg2 := libaggregate.Aggregate{
		Name: "second_agg", Query: getQuery(), Timestamp: 123,
		Source: libaggregate.SOURCE_ACTION,
		Mode:   "rql",
		Options: libaggregate.Options{
			AggType:   "max",
			Durations: []uint32{6 * 3600},
		},
		Id: 2,
	}
	t0 := uint32(24 * 3600)
	t1 := clock.Now().Add(time.Duration(t0) * time.Second)
	clock.Set(t1)
	assert.NoError(t, aggregate.Store(ctx, tier, agg))
	assert.NoError(t, aggregate.Store(ctx, tier, agg2))

	// wait for the aggregates to be consumed
	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	uids := []ftypes.OidType{"1", "2", "1"}
	var actions []action.Action
	var err error
	for i := 0; i < 3; i++ {
		a := getAction(uids[i], ftypes.Timestamp(t0), "like")
		actions = append(actions, a)
		err = controller_action.Insert(ctx, tier, a)
		assert.NoError(t, err)
	}
	// Insert one action at a later timestamp
	a := getAction("2", ftypes.Timestamp(t0+1800), "like")
	actions = append(actions, a)
	err = controller_action.Insert(ctx, tier, a)
	assert.NoError(t, err)
	clock.Set(t1.Add(3600 * time.Second))
	assert.NoError(t, aggregate.Update(ctx, tier, actions, agg))
	assert.NoError(t, aggregate.Update(ctx, tier, actions, agg2))

	// wait for the actions to be consumed
	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	found, err := aggregate.Value(ctx, tier, agg.Name, value.Int(1), value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(2), found)
	found, err = aggregate.Value(ctx, tier, agg.Name, value.Int(2), value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(2), found)
	found, err = aggregate.Value(ctx, tier, agg.Name, value.Int(2), value.NewDict(map[string]value.Value{"duration": value.Int(2000)}))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(1), found)

	static := value.NewDict(map[string]value.Value{"field": value.String("myaggresults")})
	inputs := []value.Value{
		value.NewDict(map[string]value.Value{"a": value.String("hi")}),
		value.NewDict(map[string]value.Value{"a": value.String("bye")}),
		value.NewDict(map[string]value.Value{"a": value.String("yo")}),
		value.NewDict(map[string]value.Value{"a": value.String("abc")}),
		value.NewDict(map[string]value.Value{"a": value.String("def")}),
	}
	contextKwargs := []value.Dict{
		value.NewDict(map[string]value.Value{"name": value.String(agg.Name), "groupkey": value.Int(1), "kwargs": value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)})}),
		value.NewDict(map[string]value.Value{"name": value.String(agg.Name), "groupkey": value.Int(2), "kwargs": value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)})}),
		value.NewDict(map[string]value.Value{"name": value.String(agg.Name), "groupkey": value.Int(3), "kwargs": value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)})}),
		value.NewDict(map[string]value.Value{"name": value.String(agg.Name), "groupkey": value.Int(2), "kwargs": value.NewDict(map[string]value.Value{"duration": value.Int(2000)})}),
		value.NewDict(map[string]value.Value{"name": value.String(agg2.Name), "groupkey": value.Int(1), "kwargs": value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)})}),
	}
	outputs := []value.Value{
		value.NewDict(map[string]value.Value{"a": value.String("hi"), "myaggresults": value.Int(2)}),
		value.NewDict(map[string]value.Value{"a": value.String("bye"), "myaggresults": value.Int(2)}),
		value.NewDict(map[string]value.Value{"a": value.String("yo"), "myaggresults": value.Int(0)}),
		value.NewDict(map[string]value.Value{"a": value.String("abc"), "myaggresults": value.Int(1)}),
		value.NewDict(map[string]value.Value{"a": value.String("def"), "myaggresults": value.Int(1)}),
	}
	optest.AssertEqual(t, tier, &AggValue{tier}, static, [][]value.Value{inputs}, contextKwargs, outputs)

	static = value.NewDict(nil)
	outputs = []value.Value{
		value.Int(2),
		value.Int(2),
		value.Int(0),
		value.Int(1),
		value.Int(1),
	}
	optest.AssertEqual(t, tier, &AggValue{tier}, static, [][]value.Value{inputs}, contextKwargs, outputs)
}

func getQuery() ast.Ast {
	return &ast.OpCall{
		Namespace: "std",
		Name:      "set",
		Operands: []ast.Ast{&ast.OpCall{
			Namespace: "std",
			Name:      "set",
			Operands:  []ast.Ast{&ast.Var{Name: "actions"}},
			Vars:      []string{"a"},
			Kwargs: &ast.Dict{Values: map[string]ast.Ast{
				"field": ast.MakeString("groupkey"),
				"value": &ast.Lookup{
					On:       &ast.Var{Name: "a"},
					Property: "actor_id",
				}},
			},
		}},
		Kwargs: &ast.Dict{Values: map[string]ast.Ast{
			"field": ast.MakeString("value"),
			"value": ast.MakeInt(1),
		}},
	}
}

func getAction(uid ftypes.OidType, ts ftypes.Timestamp, actionType ftypes.ActionType) action.Action {
	return action.Action{
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
