package main

import (
	"fennel/controller/action"
	"fennel/controller/aggregate"
	"fennel/engine/ast"
	"fennel/instance"
	actionlib "fennel/lib/action"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEndToEnd2(t *testing.T) {
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)
	clock := &test.FakeClock{}
	instance.Clock = clock

	agg1 := libaggregate.Aggregate{
		CustID: instance.CustID, Type: "rolling_counter", Name: "counter1", Query: getQuery(),
		Timestamp: 123, Options: libaggregate.AggOptions{Duration: 6 * 3600},
	}
	assert.NoError(t, aggregate.Store(instance, agg1))
	agg2 := libaggregate.Aggregate{
		CustID: instance.CustID, Type: "timeseries_counter", Name: "timeseries", Query: getQuery(),
		Timestamp: 123, Options: libaggregate.AggOptions{Window: ftypes.Window_HOUR, Limit: 4},
	}
	assert.NoError(t, aggregate.Store(instance, agg2))
	uid1 := ftypes.OidType(1312)
	uid2 := ftypes.OidType(8312)
	key1 := value.Int(uid1)
	key2 := value.Int(uid2)

	t0 := ftypes.Timestamp(time.Hour * 24 * 15)
	clock.Set(int64(t0))
	// Initially count for keys are zero/empty
	verify(t, instance, agg1, key1, value.Int(0))
	verify(t, instance, agg1, key2, value.Int(0))
	verify(t, instance, agg2, key1, value.List{value.Int(0), value.Int(0), value.Int(0), value.Int(0)})
	verify(t, instance, agg2, key2, value.List{value.Int(0), value.Int(0), value.Int(0), value.Int(0)})

	// now fire a few actions and run action processing
	for i := 1; i <= 4; i += 1 {
		if i%2 == 0 {
			logAction(t, instance, uid1, t0+ftypes.Timestamp(i))
		} else {
			logAction(t, instance, uid2, t0+ftypes.Timestamp(i))
		}
	}
	t1 := t0 + 7200
	clock.Set(int64(t1))
	// counts don't change until we run process, after which, they do
	verify(t, instance, agg1, key1, value.Int(0))
	verify(t, instance, agg1, key2, value.Int(0))
	verify(t, instance, agg2, key1, value.List{value.Int(0), value.Int(0), value.Int(0), value.Int(0)})
	verify(t, instance, agg2, key2, value.List{value.Int(0), value.Int(0), value.Int(0), value.Int(0)})
	processOnce(instance)

	// now the counts should be two each for each key (note: actions that were fired as share didn't count)
	verify(t, instance, agg1, key1, value.Int(2))
	verify(t, instance, agg1, key2, value.Int(2))
	verify(t, instance, agg2, key1, value.List{value.Int(0), value.Int(0), value.Int(2), value.Int(0)})
	verify(t, instance, agg2, key2, value.List{value.Int(0), value.Int(0), value.Int(2), value.Int(0)})

	// add one more action but only from uid1
	logAction(t, instance, uid1, t1+ftypes.Timestamp(1))
	processOnce(instance)

	t2 := t1 + 3*3600
	clock.Set(int64(t2))
	verify(t, instance, agg1, key1, value.Int(3))
	verify(t, instance, agg1, key2, value.Int(2))
	verify(t, instance, agg2, key1, value.List{value.Int(0), value.Int(1), value.Int(0), value.Int(0)})
	verify(t, instance, agg2, key2, value.List{value.Int(0), value.Int(0), value.Int(0), value.Int(0)})
}

func verify(t *testing.T, instance instance.Instance, agg libaggregate.Aggregate, k value.Value, expected interface{}) {
	found, err := aggregate.Value(instance, agg.Type, agg.Name, k)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}

func logAction(t *testing.T, instance instance.Instance, uid ftypes.OidType, ts ftypes.Timestamp) {
	a1 := actionlib.Action{
		CustID:      instance.CustID,
		ActorID:     uid,
		ActorType:   "user",
		TargetID:    10,
		TargetType:  "video",
		ActionType:  "like",
		ActionValue: 3,
		Timestamp:   ts,
		RequestID:   12,
	}
	a2 := a1
	a2.ActionType = "share"
	_, err := action.Insert(instance, a1)
	assert.NoError(t, err)
	_, err = action.Insert(instance, a2)
	assert.NoError(t, err)
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
		Name:      "addColumn",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"name": ast.MakeString("key"),
			"value": ast.Lookup{
				On:       ast.At{},
				Property: "actor_id",
			}},
		},
	}
}
