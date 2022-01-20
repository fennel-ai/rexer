package main

import (
	"fennel/client"
	"fennel/data/lib"
	"fennel/lib/action"
	profileLib "fennel/lib/profile"
	"fennel/value"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func verifyFetch(t *testing.T, c client.Client, request action.ActionFetchRequest, expected []action.Action) {
	found, err := c.FetchActions(request)
	assert.NoError(t, err)
	equals(t, expected, found)
}

func equals(t *testing.T, expected []action.Action, found []action.Action) {
	// we don't test equality of found/expected directly
	// action_ids aren't set properly in expected yet
	// instead, we use Equals method on Action struct and tell it to ignore IDs
	assert.Equal(t, len(expected), len(found))
	for i, e := range expected {
		assert.True(t, e.Equals(found[i], true))
	}
}

func add(controller MainController, t *testing.T, c client.Client, a action.Action) action.Action {
	err := c.LogAction(a)
	assert.NoError(t, err)

	// and also make one run of "LogAction" on server to ensure that the message goes through
	err = controller.TailActions()
	assert.NoError(t, err)
	return a
}

func TestServerClientBasic(t *testing.T) {
	controller, err := DefaultMainController()
	assert.NoError(t, err)
	// create a server
	// one goroutine will run http server
	go serve(controller)
	defer shutDownServer()
	// create a client
	c := client.NewClient("http://localhost")

	// in the beginning, with no value set, we get []actions but with no error
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{})

	// now we add a couple of actions
	a := action.Action{ActorType: 1, ActorID: 2, ActionType: 3, TargetType: 4, TargetID: 5}
	// logging this should fail because some fields (e.g. requestID aren't specified)
	err = c.LogAction(a)
	assert.Error(t, err)
	// and no action was logged on server even after process
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{})

	// but this error disappears when we pass all values
	action1 := add(controller, t, c, action.Action{
		ActorType: 1, ActorID: 2, ActionType: 3, TargetType: 4, TargetID: 5, RequestID: 6, Timestamp: 7},
	)
	// and this action should show up in requests
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{action1})

	// add a couple of actions
	action2 := add(controller, t, c, action.Action{
		ActorType: 11, ActorID: 12, ActionType: 13, TargetType: 14, TargetID: 15, RequestID: 16, Timestamp: 17},
	)
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{action1, action2})
	action3 := add(controller, t, c, action.Action{
		ActorType: 22, ActorID: 23, ActionType: 23, TargetType: 24, TargetID: 25, RequestID: 26, Timestamp: 27},
	)
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{action1, action2, action3})
}

func TestEndToEnd(t *testing.T) {
	controller, err := DefaultMainController()
	assert.NoError(t, err)

	// start the server
	go serve(controller)
	defer shutDownServer()
	// and create a client
	c := client.NewClient("http://localhost")

	// Initially count for keys are zero
	uid := profileLib.OidType(1)
	video_id := profileLib.OidType(2)
	ts := action.Timestamp(123)

	requests := []lib.GetCountRequest{
		{lib.CounterType_USER_LIKE, lib.Window_HOUR, lib.Key{uid}, ts},
		{lib.CounterType_USER_LIKE, lib.Window_DAY, lib.Key{uid}, ts},
		{lib.CounterType_USER_LIKE, lib.Window_WEEK, lib.Key{uid}, ts},
		{lib.CounterType_USER_LIKE, lib.Window_MONTH, lib.Key{uid}, ts},
		{lib.CounterType_USER_LIKE, lib.Window_QUARTER, lib.Key{uid}, ts},
		{lib.CounterType_USER_LIKE, lib.Window_YEAR, lib.Key{uid}, ts},
		{lib.CounterType_USER_LIKE, lib.Window_FOREVER, lib.Key{uid}, ts},
		{lib.CounterType_VIDEO_LIKE, lib.Window_HOUR, lib.Key{video_id}, ts},
		{lib.CounterType_VIDEO_LIKE, lib.Window_DAY, lib.Key{video_id}, ts},
		{lib.CounterType_VIDEO_LIKE, lib.Window_WEEK, lib.Key{video_id}, ts},
		{lib.CounterType_VIDEO_LIKE, lib.Window_MONTH, lib.Key{video_id}, ts},
		{lib.CounterType_VIDEO_LIKE, lib.Window_QUARTER, lib.Key{video_id}, ts},
		{lib.CounterType_VIDEO_LIKE, lib.Window_YEAR, lib.Key{video_id}, ts},
		{lib.CounterType_VIDEO_LIKE, lib.Window_FOREVER, lib.Key{video_id}, ts},
		{lib.CounterType_USER_VIDEO_LIKE, lib.Window_HOUR, lib.Key{uid, video_id}, ts},
		{lib.CounterType_USER_VIDEO_LIKE, lib.Window_DAY, lib.Key{uid, video_id}, ts},
		{lib.CounterType_USER_VIDEO_LIKE, lib.Window_WEEK, lib.Key{uid, video_id}, ts},
		{lib.CounterType_USER_VIDEO_LIKE, lib.Window_MONTH, lib.Key{uid, video_id}, ts},
		{lib.CounterType_USER_VIDEO_LIKE, lib.Window_QUARTER, lib.Key{uid, video_id}, ts},
		{lib.CounterType_USER_VIDEO_LIKE, lib.Window_YEAR, lib.Key{uid, video_id}, ts},
		{lib.CounterType_USER_VIDEO_LIKE, lib.Window_FOREVER, lib.Key{uid, video_id}, ts},
	}
	for _, cr := range requests {
		count, err := c.GetCount(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), count)
	}

	// now make an event
	action1 := action.Action{ActorType: profileLib.User, ActorID: uid, TargetType: profileLib.Video, TargetID: video_id, ActionType: action.Like, RequestID: 1, Timestamp: ts}
	add(controller, t, c, action1)

	// and verify it went through
	verifyFetch(t, c, action.ActionFetchRequest{MinActionID: 0}, []action.Action{action1})

	// and all counts should still be zero
	for _, cr := range requests {
		count, err := c.GetCount(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), count)
	}
	// but counts should go up after we aggregate
	aggregate(controller)
	for _, cr := range requests {
		count, err := c.GetCount(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count, cr)
	}
	// now make one more event which is not of matching action type (Like vs Share)
	action2 := action.Action{ActorType: profileLib.User, ActorID: uid, TargetType: profileLib.Video, TargetID: video_id, ActionType: action.Share, RequestID: 1, Timestamp: ts}
	add(controller, t, c, action2)
	verifyFetch(t, c, action.ActionFetchRequest{MinActionID: 0}, []action.Action{action1, action2})

	// this one doesn't change the counts because action type doesn't match
	aggregate(controller)
	for _, cr := range requests {
		count, err := c.GetCount(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count, cr)
	}

	// but another valid event will make the counts go up
	action3 := action.Action{ActorType: profileLib.User, ActorID: uid, TargetType: profileLib.Video, TargetID: video_id, ActionType: action.Like, RequestID: 1, Timestamp: ts}
	add(controller, t, c, action3)
	verifyFetch(t, c, action.ActionFetchRequest{MinActionID: 0}, []action.Action{action1, action2, action3})

	aggregate(controller)
	for _, cr := range requests {
		count, err := c.GetCount(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), count, cr)
	}

	// and finally we can also do rate checks
	rr := lib.GetRateRequest{
		lib.CounterType_USER_LIKE,
		lib.CounterType_VIDEO_LIKE,
		lib.Key{uid},
		lib.Key{video_id},
		lib.Window_HOUR,
		ts,
		true,
	}
	rate, err := c.GetRate(rr)
	assert.NoError(t, err)
	assert.Equal(t, 0.3423719528896193, rate)
	// and verify upper bound too
	rr.LowerBound = false
	rate, err = c.GetRate(rr)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, rate)
}

// TODO: add more tests covering more error conditions
func TestProfile(t *testing.T) {
	controller, err := DefaultMainController()
	assert.NoError(t, err)

	// start the server
	go serve(controller)
	defer shutDownServer()
	// and create a client
	c := client.NewClient(fmt.Sprintf("http://localhost"))

	// in the beginning, with no value set, we get nil pointer back but with no error
	checkGetSet(t, c, true, 1, 1, 0, "age", value.Value(nil))

	var expected value.Value = value.List([]value.Value{value.Int(1), value.Bool(false), value.Nil})
	checkGetSet(t, c, false, 1, 1, 1, "age", expected)

	// we can also get it without using the specific version number
	checkGetSet(t, c, true, 1, 1, 0, "age", expected)

	// set few more key/value pairs and verify it works
	checkGetSet(t, c, false, 1, 1, 2, "age", value.Nil)
	checkGetSet(t, c, false, 1, 3, 2, "age", value.Int(1))
	checkGetSet(t, c, true, 1, 1, 2, "age", value.Nil)
	checkGetSet(t, c, true, 1, 1, 0, "age", value.Nil)
	checkGetSet(t, c, false, 10, 3131, 0, "summary", value.Int(1))
}

func checkGetSet(t *testing.T, c client.Client, get bool, otype uint32, oid uint64, version uint64,
	key string, val value.Value) {
	if get {
		req := profileLib.NewProfileItem(otype, oid, key, version)
		found, err := c.GetProfile(&req)
		assert.NoError(t, err)
		if found == nil {
			assert.Equal(t, nil, val)
		} else {
			assert.Equal(t, val, *found)
		}
	} else {
		err := c.SetProfile(&profileLib.ProfileItem{OType: otype, Oid: oid, Key: key, Version: version, Value: val})
		assert.NoError(t, err)
		request := profileLib.NewProfileItem(otype, oid, key, version)
		found, err := c.GetProfile(&request)
		assert.NoError(t, err)
		if found == nil {
			assert.Equal(t, nil, val)
		} else {
			assert.Equal(t, val, *found)
		}
	}
}
