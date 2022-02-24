package aggregate

import (
	"testing"

	"fennel/engine/ast"
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
