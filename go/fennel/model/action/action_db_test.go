package action

import (
	"context"
	"testing"

	"fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

// TODO: write exhaustive tests
func TestActionDBBasic(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	var request action.ActionFetchRequest
	// initially before setting, value isn't there so we get empty response
	found, err := Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.Empty(t, found)
	// let's add an action
	action1 := action.Action{ActorID: 111, ActorType: "11", TargetType: "12", TargetID: 121, ActionType: "13", Metadata: value.Int(14), Timestamp: 15, RequestID: 16}
	action1ser, err := action1.ToActionSer()
	assert.NoError(t, err)
	aid1, err := Insert(ctx, tier, action1ser)
	assert.NoError(t, err)

	action2 := action.Action{ActorID: 211, ActorType: "21", TargetType: "22", TargetID: 221, ActionType: "23", Metadata: value.Int(24), Timestamp: 25, RequestID: 26}
	action2ser, err := action2.ToActionSer()
	assert.NoError(t, err)
	aid2, err := Insert(ctx, tier, action2ser)
	assert.NoError(t, err)

	// assign these ids to actions so we can verify we get them back
	action1.ActionID = ftypes.OidType(aid1)
	action1ser.ActionID = action1.ActionID
	action2.ActionID = ftypes.OidType(aid2)
	action2ser.ActionID = action2.ActionID

	// now we should have total two actions
	found, err = Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []action.ActionSer{*action1ser, *action2ser}, found)

	// and each of the following queries should work
	request = action.ActionFetchRequest{ActorID: action1.ActorID}
	found, err = Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []action.ActionSer{*action1ser}, found)

	request = action.ActionFetchRequest{ActorID: action2.ActorID}
	found, err = Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []action.ActionSer{*action2ser}, found)

	request = action.ActionFetchRequest{TargetType: action2.TargetType, ActionType: action1.ActionType}
	found, err = Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []action.ActionSer{}, found)

	// and this works for actionIDs too (though now min is exclusive and max is inclusive)
	request = action.ActionFetchRequest{MinActionID: action1.ActionID}
	found, err = Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []action.ActionSer{*action2ser}, found)
}

func TestInsertBatch(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	var request action.ActionFetchRequest
	// initially before setting, value isn't there so we get empty response
	found, err := Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.Empty(t, found)

	// now insert a few actions
	action1 := action.Action{ActorID: 111, ActorType: "11", TargetType: "12", TargetID: 121, ActionType: "13", Metadata: value.Int(14), Timestamp: 15, RequestID: 16}
	action1ser, err := action1.ToActionSer()
	assert.NoError(t, err)
	action2 := action.Action{ActorID: 211, ActorType: "21", TargetType: "22", TargetID: 221, ActionType: "23", Metadata: value.Int(24), Timestamp: 25, RequestID: 26}
	action2ser, err := action2.ToActionSer()
	assert.NoError(t, InsertBatch(ctx, tier, []action.ActionSer{*action1ser, *action2ser}))

	found, err = Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.Len(t, found, 2)

	f1ptr, err := found[0].ToAction()
	assert.NoError(t, err)
	assert.True(t, f1ptr.Equals(action1, true))

	f2ptr, err := found[1].ToAction()
	assert.NoError(t, err)
	assert.True(t, f2ptr.Equals(action2, true))
}
