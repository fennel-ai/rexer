package action

import (
	"testing"

	"fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

// TODO: write exhaustive tests
func TestActionDBBasic(t *testing.T) {
	this, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(this)

	var request action.ActionFetchRequest
	// initially before setting, value isn't there so we get empty response
	found, err := Fetch(this, request)
	assert.NoError(t, err)
	assert.Empty(t, found)
	// let's add an action
	action1 := action.Action{CustID: 1, ActorID: 111, ActorType: "11", TargetType: "12", TargetID: 121, ActionType: "13", ActionValue: 14, Timestamp: 15, RequestID: 16}
	aid1, err := Insert(this, action1)
	assert.NoError(t, err)

	action2 := action.Action{CustID: 1, ActorID: 211, ActorType: "21", TargetType: "22", TargetID: 221, ActionType: "23", ActionValue: 24, Timestamp: 25, RequestID: 26}
	aid2, err := Insert(this, action2)
	assert.NoError(t, err)

	// assign these ids to actions so we can verify we get them back
	action1.ActionID = ftypes.OidType(aid1)
	action2.ActionID = ftypes.OidType(aid2)

	// now we should have total two actions
	found, err = Fetch(this, request)
	assert.NoError(t, err)
	assert.Equal(t, []action.Action{action1, action2}, found)

	// and each of the following queries should work
	request = action.ActionFetchRequest{ActorID: action1.ActorID}
	found, err = Fetch(this, request)
	assert.NoError(t, err)
	assert.Equal(t, []action.Action{action1}, found)

	request = action.ActionFetchRequest{ActorID: action2.ActorID}
	found, err = Fetch(this, request)
	assert.NoError(t, err)
	assert.Equal(t, []action.Action{action2}, found)

	request = action.ActionFetchRequest{TargetType: action2.TargetType, ActionType: action1.ActionType}
	found, err = Fetch(this, request)
	assert.NoError(t, err)
	assert.Equal(t, []action.Action{}, found)

	// minActionValue etc also work (both min/max are inclusive)
	request = action.ActionFetchRequest{MinActionValue: action1.ActionValue, MaxActionValue: action2.ActionValue}
	found, err = Fetch(this, request)
	assert.NoError(t, err)
	assert.Equal(t, []action.Action{action1, action2}, found)

	request = action.ActionFetchRequest{MinActionValue: action1.ActionValue + 1}
	found, err = Fetch(this, request)
	assert.NoError(t, err)
	assert.Equal(t, []action.Action{action2}, found)

	// and this works for actionIDs too (though now min is exclusive and max is inclusive)
	request = action.ActionFetchRequest{MinActionID: action1.ActionID}
	found, err = Fetch(this, request)
	assert.NoError(t, err)
	assert.Equal(t, []action.Action{action2}, found)
}

func TestLongTypes(t *testing.T) {
	this, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(this)

	// valid action
	action1 := action.Action{
		CustID:      1,
		ActorID:     111,
		ActorType:   "11",
		TargetType:  "12",
		TargetID:    121,
		ActionType:  "13",
		ActionValue: 14,
		Timestamp:   15,
		RequestID:   16,
	}

	// ActionType can't be longer than 256 chars
	action1.ActionType = ftypes.ActionType(utils.RandString(257))
	_, err = Insert(this, action1)
	assert.Error(t, err)
	action1.ActionType = ftypes.ActionType(utils.RandString(256))

	// ActorType can't be longer than 256 chars
	action1.ActorType = ftypes.OType(utils.RandString(257))
	_, err = Insert(this, action1)
	assert.Error(t, err)
	action1.ActorType = ftypes.OType(utils.RandString(256))

	// TargetType can't be longer than 256 chars
	action1.TargetType = ftypes.OType(utils.RandString(257))
	_, err = Insert(this, action1)
	assert.Error(t, err)
	action1.TargetType = ftypes.OType(utils.RandString(256))
}
