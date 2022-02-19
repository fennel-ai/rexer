package aggregate

import (
	"context"
	"testing"

	action2 "fennel/controller/action"
	"fennel/engine/ast"
	"fennel/kafka"
	"fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

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
		assert.Equal(t, value.List{value.Int(uid)}, row["key"])
	}
}

func Test_ReadActions(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	actions := make([]action.Action, 0)
	uid := ftypes.OidType(41)
	for i := 0; i < 100; i++ {
		a1 := getAction(i, uid, ftypes.Timestamp(i+1000), "like")
		a2 := getAction(i, uid, ftypes.Timestamp(i+1005), "share")
		aid, err := action2.Insert(ctx, tier, a1)
		assert.NoError(t, err)
		a1.ActionID = ftypes.OidType(aid)
		aid, err = action2.Insert(ctx, tier, a2)
		assert.NoError(t, err)
		a2.ActionID = ftypes.OidType(aid)
		actions = append(actions, a1, a2)
	}
	c1, err := tier.NewKafkaConsumer(action.ACTIONLOG_KAFKA_TOPIC, "one", kafka.DefaultOffsetPolicy)
	defer c1.Close()
	assert.NoError(t, err)
	c2, err := tier.NewKafkaConsumer(action.ACTIONLOG_KAFKA_TOPIC, "two", kafka.DefaultOffsetPolicy)
	assert.NoError(t, err)
	defer c2.Close()

	// verify both c1 and c2 produce the same actions
	found1, err := readActions(ctx, c1)
	assert.NoError(t, err)
	assert.Equal(t, found1, actions)

	found2, err := readActions(ctx, c2)
	assert.NoError(t, err)
	assert.Equal(t, found2, actions)
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
			"name": ast.MakeString("key"),
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
