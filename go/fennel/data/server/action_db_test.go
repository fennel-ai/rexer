package main

import (
	"fennel/data/lib"
	"fennel/instance"
	profileLib "fennel/profile/lib"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TODO: write exhaustive tests
func TestActionDBBasic(t *testing.T) {
	err := instance.Setup([]instance.Resource{instance.DB})
	assert.NoError(t, err)

	var request lib.ActionFetchRequest
	// initially before setting, value isn't there so we get empty response
	found, err := actionDBGet(request)
	assert.NoError(t, err)
	assert.Empty(t, found)
	// let's add an action
	action1 := lib.Action{ActorID: 111, ActorType: 11, TargetType: 12, TargetID: 121, ActionType: 13, ActionValue: 14, Timestamp: 15, RequestID: 16}
	aid1, err := actionDBInsert(action1)
	assert.NoError(t, err)

	action2 := lib.Action{ActorID: 211, ActorType: 21, TargetType: 22, TargetID: 221, ActionType: 23, ActionValue: 24, Timestamp: 25, RequestID: 26}
	aid2, err := actionDBInsert(action2)
	assert.NoError(t, err)

	// assign these ids to actions so we can verify we get them back
	action1.ActionID = profileLib.OidType(aid1)
	action2.ActionID = profileLib.OidType(aid2)

	// now we should have total two actions
	found, err = actionDBGet(request)
	assert.NoError(t, err)
	assert.Equal(t, []lib.Action{action1, action2}, found)

	// and each of the following queries should work
	request = lib.ActionFetchRequest{ActorID: action1.ActorID}
	found, err = actionDBGet(request)
	assert.NoError(t, err)
	assert.Equal(t, []lib.Action{action1}, found)

	request = lib.ActionFetchRequest{ActorID: action2.ActorID}
	found, err = actionDBGet(request)
	assert.NoError(t, err)
	assert.Equal(t, []lib.Action{action2}, found)

	request = lib.ActionFetchRequest{TargetType: action2.TargetType, ActionType: action1.ActionType}
	found, err = actionDBGet(request)
	assert.NoError(t, err)
	assert.Equal(t, []lib.Action{}, found)

	// minActionValue etc also work (both min/max are inclusive)
	request = lib.ActionFetchRequest{MinActionValue: action1.ActionValue, MaxActionValue: action2.ActionValue}
	found, err = actionDBGet(request)
	assert.NoError(t, err)
	assert.Equal(t, []lib.Action{action1, action2}, found)

	request = lib.ActionFetchRequest{MinActionValue: action1.ActionValue + 1}
	found, err = actionDBGet(request)
	assert.NoError(t, err)
	assert.Equal(t, []lib.Action{action2}, found)

	// and this works for actionIDs too (though now min is exclusive and max is inclusive)
	request = lib.ActionFetchRequest{MinActionID: action1.ActionID}
	found, err = actionDBGet(request)
	assert.NoError(t, err)
	assert.Equal(t, []lib.Action{action2}, found)
}
