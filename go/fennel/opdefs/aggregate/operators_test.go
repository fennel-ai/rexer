package aggregate

import (
	"context"
	"testing"

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
	"fennel/test/optest"
)

func TestAggValue_Apply(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()

	// create an aggregate, store it, create some actions, update aggregate and ensure its value returns some result
	agg := libaggregate.Aggregate{
		Name: "counter1", Query: getQuery(), Timestamp: 123,
		Options: libaggregate.Options{
			AggType:   "sum",
			Durations: []uint64{6 * 3600, 3 * 3600, 2000},
		},
		Id: 1,
	}
	agg2 := libaggregate.Aggregate{
		Name: "second_agg", Query: getQuery(), Timestamp: 123,
		Options: libaggregate.Options{
			AggType:   "max",
			Durations: []uint64{6 * 3600},
		},
		Id: 2,
	}
	t0 := int64(24 * 3600)
	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(t0)
	assert.NoError(t, aggregate.Store(ctx, tier, agg))
	assert.NoError(t, aggregate.Store(ctx, tier, agg2))

	uids := []ftypes.OidType{"1", "2", "1"}
	for i := 0; i < 3; i++ {
		a := getAction(uids[i], ftypes.Timestamp(t0), "like")
		err = controller_action.Insert(ctx, tier, a)
		assert.NoError(t, err)
	}
	// Insert one action at a later timestamp
	a := getAction("2", ftypes.Timestamp(t0+1800), "like")
	err = controller_action.Insert(ctx, tier, a)
	assert.NoError(t, err)
	clock.Set(t0 + 3600)
	consumer, err := tier.NewKafkaConsumer(action.ACTIONLOG_KAFKA_TOPIC, string(agg.Name), "earliest")
	defer consumer.Close()
	consumer2, err := tier.NewKafkaConsumer(action.ACTIONLOG_KAFKA_TOPIC, string(agg2.Name), "earliest")
	defer consumer2.Close()
	assert.NoError(t, err)
	assert.NoError(t, aggregate.Update(ctx, tier, consumer, agg))
	assert.NoError(t, aggregate.Update(ctx, tier, consumer2, agg2))
	found, err := aggregate.Value(ctx, tier, agg.Name, value.String("1"), value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(2), found)
	found, err = aggregate.Value(ctx, tier, agg.Name, value.String("2"), value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(2), found)
	found, err = aggregate.Value(ctx, tier, agg.Name, value.String("2"), value.NewDict(map[string]value.Value{"duration": value.Int(2000)}))
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
		value.NewDict(map[string]value.Value{"name": value.String(agg.Name), "groupkey": value.String("1"), "kwargs": value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)})}),
		value.NewDict(map[string]value.Value{"name": value.String(agg.Name), "groupkey": value.String("2"), "kwargs": value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)})}),
		value.NewDict(map[string]value.Value{"name": value.String(agg.Name), "groupkey": value.String("3"), "kwargs": value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)})}),
		value.NewDict(map[string]value.Value{"name": value.String(agg.Name), "groupkey": value.String("2"), "kwargs": value.NewDict(map[string]value.Value{"duration": value.Int(2000)})}),
		value.NewDict(map[string]value.Value{"name": value.String(agg2.Name), "groupkey": value.String("1"), "kwargs": value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)})}),
	}
	outputs := []value.Value{
		value.NewDict(map[string]value.Value{"a": value.String("hi"), "myaggresults": value.Int(2)}),
		value.NewDict(map[string]value.Value{"a": value.String("bye"), "myaggresults": value.Int(2)}),
		value.NewDict(map[string]value.Value{"a": value.String("yo"), "myaggresults": value.Int(0)}),
		value.NewDict(map[string]value.Value{"a": value.String("abc"), "myaggresults": value.Int(1)}),
		value.NewDict(map[string]value.Value{"a": value.String("def"), "myaggresults": value.Int(1)}),
	}
	optest.AssertEqual(t, tier, &AggValue{tier}, static, [][]value.Value{inputs}, contextKwargs, outputs)

	static = value.NewDict(map[string]value.Value{})
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
	return ast.OpCall{
		Namespace: "std",
		Name:      "set",
		Operands: []ast.Ast{ast.OpCall{
			Namespace: "std",
			Name:      "set",
			Operands:  []ast.Ast{ast.Var{Name: "actions"}},
			Vars:      []string{"a"},
			Kwargs: ast.Dict{Values: map[string]ast.Ast{
				"field": ast.MakeString("groupkey"),
				"value": ast.Lookup{
					On:       ast.Var{Name: "a"},
					Property: "actor_id",
				}},
			},
		}},
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
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
		RequestID:  7,
	}
}
