package action

import (
	"context"
	"strconv"
	"testing"

	"fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

// TODO: write exhaustive tests
func TestActionDBBasic(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	var request action.ActionFetchRequest
	// initially before setting, value isn't there so we get empty response
	found, err := Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.Empty(t, found)
	// let's add some actions.
	action1 := action.Action{ActorID: "111", ActorType: "11", TargetType: "12", TargetID: "121", ActionType: "13", Metadata: value.Int(14), Timestamp: 15, RequestID: "16"}
	action2 := action.Action{ActorID: "211", ActorType: "21", TargetType: "22", TargetID: "221", ActionType: "23", Metadata: value.Int(24), Timestamp: 25, RequestID: "26"}
	err = InsertBatch(ctx, tier, []action.Action{action1, action2})
	assert.NoError(t, err)

	// now we should have total two actions
	found, err = Fetch(ctx, tier, request)
	assert.NoError(t, err)
	// Just copy the action ids since those are only assigned after insert.
	action2.ActionID = found[0].ActionID
	action1.ActionID = found[1].ActionID
	assert.ElementsMatch(t, []action.Action{action2, action1}, found)

	// and each of the following queries should work
	request = action.ActionFetchRequest{ActorID: action1.ActorID}
	found, err = Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []action.Action{action1}, found)

	request = action.ActionFetchRequest{ActorID: action2.ActorID}
	found, err = Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []action.Action{action2}, found)

	request = action.ActionFetchRequest{TargetType: action2.TargetType, ActionType: action1.ActionType}
	found, err = Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.Empty(t, found)

	// and this works for actionIDs too (though now min is exclusive and max is inclusive)
	request = action.ActionFetchRequest{MinActionID: action1.ActionID}
	found, err = Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []action.Action{action2}, found)
}

func TestInsertBatch(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	var request action.ActionFetchRequest
	// initially before setting, value isn't there so we get empty response
	found, err := Fetch(ctx, tier, request)
	assert.NoError(t, err)
	assert.Empty(t, found)

	// now insert a few actions
	action1 := action.Action{ActorID: "111", ActorType: "11", TargetType: "12", TargetID: "121", ActionType: "13", Metadata: value.Int(14), Timestamp: 15, RequestID: "16"}
	action2 := action.Action{ActorID: "211", ActorType: "21", TargetType: "22", TargetID: "221", ActionType: "23", Metadata: value.Int(24), Timestamp: 25, RequestID: "26"}
	assert.NoError(t, InsertBatch(ctx, tier, []action.Action{action1, action2}))

	found, err = Fetch(ctx, tier, request)
	// Just copy the action ids since those are only assigned after insert.
	action2.ActionID = found[0].ActionID
	action1.ActionID = found[1].ActionID
	assert.NoError(t, err)
	assert.Len(t, found, 2)
	assert.Equal(t, []action.Action{action2, action1}, found)
}

func TestAction_ToActionSer(t *testing.T) {
	a := makeTestAction(1)
	aSer := serializeAction(a)
	assert.Equal(t, makeTestActionSer(1), aSer)
}

func TestActionSer_ToAction(t *testing.T) {
	aSer := makeTestActionSer(2)
	a, err := deserialize(aSer)
	assert.NoError(t, err)
	assert.Equal(t, []action.Action{makeTestAction(2)}, a)
}

func TestFromActionSerList(t *testing.T) {
	alSer := make([]actionSer, 10)
	expected := make([]action.Action, 10)
	for i := 0; i < 10; i++ {
		alSer[i] = makeTestActionSer(i)
		expected[i] = makeTestAction(i)
	}
	al, err := deserialize(alSer...)
	assert.NoError(t, err)
	assert.Equal(t, expected, al)
}

func makeTestAction(k int) action.Action {
	k *= 20
	return action.Action{
		ActionID:   ftypes.IDType(k),
		ActorID:    ftypes.OidType(strconv.Itoa(k + 1)),
		ActorType:  ftypes.OType(strconv.Itoa(k + 2)),
		TargetID:   ftypes.OidType(strconv.Itoa(k + 3)),
		TargetType: ftypes.OType(strconv.Itoa(k + 4)),
		ActionType: ftypes.ActionType(strconv.Itoa(k + 5)),
		Timestamp:  ftypes.Timestamp(k + 6),
		RequestID:  ftypes.RequestID(strconv.Itoa(k + 7)),
		Metadata:   value.Double(k + 8),
	}
}
func makeTestActionSer(k int) actionSer {
	k *= 20
	return actionSer{
		ActionID:   ftypes.IDType(k),
		ActorID:    ftypes.OidType(strconv.Itoa(k + 1)),
		ActorType:  ftypes.OType(strconv.Itoa(k + 2)),
		TargetID:   ftypes.OidType(strconv.Itoa(k + 3)),
		TargetType: ftypes.OType(strconv.Itoa(k + 4)),
		ActionType: ftypes.ActionType(strconv.Itoa(k + 5)),
		Timestamp:  ftypes.Timestamp(k + 6),
		RequestID:  ftypes.RequestID(strconv.Itoa(k + 7)),
		Metadata:   []byte(strconv.Itoa(k+8) + ".0"),
	}
}
