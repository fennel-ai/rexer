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
	_ "fennel/opdefs/std"
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
			AggType:  "sum",
			Duration: 6 * 3600,
		},
	}
	t0 := int64(24 * 3600)
	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(t0)
	assert.NoError(t, aggregate.Store(ctx, tier, agg))

	uids := []ftypes.OidType{1, 2, 1}
	for i := 0; i < 3; i++ {
		a := getAction(uids[i], ftypes.Timestamp(t0), "like")
		err = controller_action.Insert(ctx, tier, a)
		assert.NoError(t, err)
	}
	// Insert one action at a later timestamp
	a := getAction(2, ftypes.Timestamp(t0+1800), "like")
	err = controller_action.Insert(ctx, tier, a)
	assert.NoError(t, err)
	clock.Set(t0 + 3600)
	consumer, err := tier.NewKafkaConsumer(action.ACTIONLOG_KAFKA_TOPIC, string(agg.Name), "earliest")
	defer consumer.Close()
	assert.NoError(t, err)
	assert.NoError(t, aggregate.Update(ctx, tier, consumer, agg))
	found, err := aggregate.Value(ctx, tier, agg.Name, value.Int(1), value.Dict{})
	assert.NoError(t, err)
	assert.Equal(t, value.Int(2), found)
	found, err = aggregate.Value(ctx, tier, agg.Name, value.Int(2), value.Dict{})
	assert.NoError(t, err)
	assert.Equal(t, value.Int(2), found)
	found, err = aggregate.Value(ctx, tier, agg.Name, value.Int(2), value.Dict{"duration": value.Int(2000)})
	assert.NoError(t, err)
	assert.Equal(t, value.Int(1), found)

	static := value.Dict{"name": value.String("myaggresults"), "aggregate": value.String(agg.Name)}
	inputs := []value.Dict{
		{"a": value.String("hi")},
		{"a": value.String("bye")},
		{"a": value.String("yo")},
		{"a": value.String("kwargs")},
	}
	contextKwargs := []value.Dict{
		{"groupkey": value.Int(1), "kwargs": value.Dict{}},
		{"groupkey": value.Int(2), "kwargs": value.Dict{}},
		{"groupkey": value.Int(3), "kwargs": value.Dict{}},
		{"groupkey": value.Int(2), "kwargs": value.Dict{"duration": value.Int(2000)}},
	}
	outputs := []value.Dict{
		{"a": value.String("hi"), "myaggresults": value.Int(2)},
		{"a": value.String("bye"), "myaggresults": value.Int(2)},
		{"a": value.String("yo"), "myaggresults": value.Int(0)},
		{"a": value.String("kwargs"), "myaggresults": value.Int(1)},
	}
	optest.Assert(t, tier, &AggValue{tier}, static, inputs, contextKwargs, outputs)
}

func getQuery() ast.Ast {
	return ast.OpCall{
		Operand: ast.OpCall{
			Operand:   ast.Lookup{On: ast.Var{Name: "args"}, Property: "actions"},
			Namespace: "std",
			Name:      "addField",
			Kwargs: ast.Dict{Values: map[string]ast.Ast{
				"name": ast.MakeString("groupkey"),
				"value": ast.Lookup{
					On:       ast.At{},
					Property: "actor_id",
				}},
			},
		},
		Namespace: "std",
		Name:      "addField",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"name":  ast.MakeString("value"),
			"value": ast.MakeInt(1),
		}},
	}
}

func getAction(uid ftypes.OidType, ts ftypes.Timestamp, actionType ftypes.ActionType) action.Action {
	return action.Action{
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
