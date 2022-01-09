package main

import (
	"fennel/client"
	"fennel/data/lib"
	"fennel/instance"
	"fennel/value"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func verifyFetch(t *testing.T, c client.Client, request lib.ActionFetchRequest, expected []lib.Action) {
	found, err := c.FetchActions(request)
	assert.NoError(t, err)
	equals(t, expected, found)
}

func equals(t *testing.T, expected []lib.Action, found []lib.Action) {
	// we don't test equality of found/expected directly
	// action_ids aren't set properly in expected yet
	// instead, we use Equals method on Action struct and tell it to ignore IDs
	assert.Equal(t, len(expected), len(found))
	for i, e := range expected {
		assert.True(t, e.Equals(found[i], true))
	}
}

func add(t *testing.T, c client.Client, a lib.Action) lib.Action {
	err := c.LogAction(a)
	assert.NoError(t, err)

	// and also make one run of "LogAction" on server to ensure that the message goes through
	err = Log()
	assert.NoError(t, err)
	return a
}

func TestAll(t *testing.T) {
	// doing this so that both these tests are forced to run one after another
	// instead of in concurrent goroutines
	_TestServerClientBasic(t)
	_TestEndToEnd(t)
}

func _TestServerClientBasic(t *testing.T) {
	err := instance.Setup([]instance.Resource{})
	assert.NoError(t, err)
	// create a server
	go serve()
	defer shutDownServer()
	// create a client
	c := client.NewClient("http://localhost")

	// in the beginning, with no value set, we get []actions but with no error
	verifyFetch(t, c, lib.ActionFetchRequest{}, []lib.Action{})

	// now we add a couple of actions
	action := lib.Action{ActorType: 1, ActorID: 2, ActionType: 3, TargetType: 4, TargetID: 5}
	// logging this should fail because some fields (e.g. requestID aren't specified)
	err = c.LogAction(action)
	assert.Error(t, err)
	// and no action was logged on server even after process
	verifyFetch(t, c, lib.ActionFetchRequest{}, []lib.Action{})

	// but this error disappears when we pass all values
	action1 := add(t, c, lib.Action{
		ActorType: 1, ActorID: 2, ActionType: 3, TargetType: 4, TargetID: 5, RequestID: 6, Timestamp: 7},
	)
	// and this action should show up in requests
	verifyFetch(t, c, lib.ActionFetchRequest{}, []lib.Action{action1})

	// add a couple of actions
	action2 := add(t, c, lib.Action{
		ActorType: 11, ActorID: 12, ActionType: 13, TargetType: 14, TargetID: 15, RequestID: 16, Timestamp: 17},
	)
	verifyFetch(t, c, lib.ActionFetchRequest{}, []lib.Action{action1, action2})
	action3 := add(t, c, lib.Action{
		ActorType: 22, ActorID: 23, ActionType: 23, TargetType: 24, TargetID: 25, RequestID: 26, Timestamp: 27},
	)
	verifyFetch(t, c, lib.ActionFetchRequest{}, []lib.Action{action1, action2, action3})
}

func _TestEndToEnd(t *testing.T) {
	err := instance.Setup([]instance.Resource{})
	assert.NoError(t, err)

	// start the server
	go serve()
	defer shutDownServer()
	// and create a client
	c := client.NewClient("http://localhost")

	// Initially count for keys are zero
	uid := lib.OidType(1)
	video_id := lib.OidType(2)
	ts := lib.Timestamp(123)

	requests := []lib.GetCountRequest{
		{lib.USER_LIKE, lib.HOUR, lib.Key{uid}, ts},
		{lib.USER_LIKE, lib.DAY, lib.Key{uid}, ts},
		{lib.USER_LIKE, lib.WEEK, lib.Key{uid}, ts},
		{lib.USER_LIKE, lib.MONTH, lib.Key{uid}, ts},
		{lib.USER_LIKE, lib.QUARTER, lib.Key{uid}, ts},
		{lib.USER_LIKE, lib.YEAR, lib.Key{uid}, ts},
		{lib.USER_LIKE, lib.FOREVER, lib.Key{uid}, ts},
		{lib.VIDEO_LIKE, lib.HOUR, lib.Key{video_id}, ts},
		{lib.VIDEO_LIKE, lib.DAY, lib.Key{video_id}, ts},
		{lib.VIDEO_LIKE, lib.WEEK, lib.Key{video_id}, ts},
		{lib.VIDEO_LIKE, lib.MONTH, lib.Key{video_id}, ts},
		{lib.VIDEO_LIKE, lib.QUARTER, lib.Key{video_id}, ts},
		{lib.VIDEO_LIKE, lib.YEAR, lib.Key{video_id}, ts},
		{lib.VIDEO_LIKE, lib.FOREVER, lib.Key{video_id}, ts},
		{lib.USER_VIDEO_LIKE, lib.HOUR, lib.Key{uid, video_id}, ts},
		{lib.USER_VIDEO_LIKE, lib.DAY, lib.Key{uid, video_id}, ts},
		{lib.USER_VIDEO_LIKE, lib.WEEK, lib.Key{uid, video_id}, ts},
		{lib.USER_VIDEO_LIKE, lib.MONTH, lib.Key{uid, video_id}, ts},
		{lib.USER_VIDEO_LIKE, lib.QUARTER, lib.Key{uid, video_id}, ts},
		{lib.USER_VIDEO_LIKE, lib.YEAR, lib.Key{uid, video_id}, ts},
		{lib.USER_VIDEO_LIKE, lib.FOREVER, lib.Key{uid, video_id}, ts},
	}
	for _, cr := range requests {
		count, err := c.GetCount(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), count)
	}

	// now make an event
	action1 := lib.Action{ActorType: lib.User, ActorID: uid, TargetType: lib.Video, TargetID: video_id, ActionType: lib.Like, RequestID: 1, Timestamp: ts}
	add(t, c, action1)

	// and verify it went through
	verifyFetch(t, c, lib.ActionFetchRequest{MinActionID: 0}, []lib.Action{action1})

	// and all counts should still be zero
	for _, cr := range requests {
		count, err := c.GetCount(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), count)
	}
	// but counts should go up after we aggregate
	aggregate()
	for _, cr := range requests {
		count, err := c.GetCount(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count, cr)
	}
	// now make one more event which is not of matching action type (Like vs Share)
	action2 := lib.Action{ActorType: lib.User, ActorID: uid, TargetType: lib.Video, TargetID: video_id, ActionType: lib.Share, RequestID: 1, Timestamp: ts}
	add(t, c, action2)
	verifyFetch(t, c, lib.ActionFetchRequest{MinActionID: 0}, []lib.Action{action1, action2})

	// this one doesn't change the counts because action type doesn't match
	aggregate()
	for _, cr := range requests {
		count, err := c.GetCount(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count, cr)
	}

	// but another valid event will make the counts go up
	action3 := lib.Action{ActorType: lib.User, ActorID: uid, TargetType: lib.Video, TargetID: video_id, ActionType: lib.Like, RequestID: 1, Timestamp: ts}
	add(t, c, action3)
	verifyFetch(t, c, lib.ActionFetchRequest{MinActionID: 0}, []lib.Action{action1, action2, action3})

	aggregate()
	for _, cr := range requests {
		count, err := c.GetCount(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), count, cr)
	}
}

// TODO: add more tests covering more error conditions
func TestProfile(t *testing.T) {
	err := instance.Setup([]instance.Resource{})
	assert.NoError(t, err)
	// start the server
	go serve()
	defer shutDownServer()
	// and create a client
	c := client.NewClient(fmt.Sprintf("http://localhost:%d", lib.PORT))

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

func checkGetSet(t *testing.T, c client.Client, get bool, otype lib.OType, oid uint64, version uint64, key string, val value.Value) {
	if get {
		found, err := c.GetProfile(otype, oid, key, version)
		assert.NoError(t, err)
		if found == nil {
			assert.Equal(t, nil, val)
		} else {
			assert.Equal(t, val, *found)
		}
	} else {
		err := c.SetProfile(otype, oid, key, version, val)
		assert.NoError(t, err)
		found, err := c.GetProfile(otype, oid, key, version)
		assert.NoError(t, err)
		if found == nil {
			assert.Equal(t, nil, val)
		} else {
			assert.Equal(t, val, *found)
		}
	}
}
