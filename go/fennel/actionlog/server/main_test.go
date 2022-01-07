package main

import (
	"fennel/actionlog/client"
	"fennel/actionlog/lib"
	"github.com/stretchr/testify/assert"
	"testing"
)

func verifyFetch(t *testing.T, c client.Client, request lib.ActionFetchRequest, expected []lib.Action) {
	found, err := c.Fetch(request)
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
	err := c.Log(a)
	assert.NoError(t, err)

	// and also make one run of "Log" on server to ensure that the message goes through
	err = Log()
	assert.NoError(t, err)
	return a
}

func TestServerClientBasic(t *testing.T) {
	t.Cleanup(dbInit)
	// create a server
	go serve()
	// create a client
	c := client.NewClient("http://localhost")

	// in the beginning, with no value set, we get []actions but with no error
	verifyFetch(t, c, lib.ActionFetchRequest{}, []lib.Action{})

	// now we add a couple of actions
	action := lib.Action{ActorType: 1, ActorID: 2, ActionType: 3, TargetType: 4, TargetID: 5}
	// logging this should fail because some fields (e.g. requestID aren't specified)
	err := c.Log(action)
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

	// verifyFetch with actionID
	verifyFetch(t, c, lib.ActionFetchRequest{MinActionID: action2.ActionID}, []lib.Action{action3})
	verifyFetch(t, c, lib.ActionFetchRequest{MinActionID: action1.ActionID, MaxActionID: action2.ActionID}, []lib.Action{action2})
}

func TestEndToEnd(t *testing.T) {
	t.Cleanup(dbInit)

	// start the server
	go serve()
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
		count, err := c.Count(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), count)
	}

	// now make an event
	action1 := lib.Action{ActorType: lib.User, ActorID: uid, TargetType: lib.Video, TargetID: video_id, ActionType: lib.Like, RequestID: 1, Timestamp: ts}
	err := c.Log(action1)
	assert.NoError(t, err)

	// and verify it went through
	verifyFetch(t, c, lib.ActionFetchRequest{MinActionID: 0}, []lib.Action{action1})

	// and all counts should still be zero
	for _, cr := range requests {
		count, err := c.Count(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), count)
	}
	// but counts should go up after we aggregate
	aggregate()
	for _, cr := range requests {
		count, err := c.Count(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count, cr)
	}
	// now make one more event which is not of matching action type (Like vs Share)
	action2 := lib.Action{ActorType: lib.User, ActorID: uid, TargetType: lib.Video, TargetID: video_id, ActionType: lib.Share, RequestID: 1, Timestamp: ts}
	err = c.Log(action2)
	assert.NoError(t, err)
	verifyFetch(t, c, lib.ActionFetchRequest{MinActionID: 0}, []lib.Action{action1, action2})

	// this one doesn't change the counts because action type doesn't match
	aggregate()
	for _, cr := range requests {
		count, err := c.Count(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count, cr)
	}

	// but another valid event will make the counts go up
	action3 := lib.Action{ActorType: lib.User, ActorID: uid, TargetType: lib.Video, TargetID: video_id, ActionType: lib.Like, RequestID: 1, Timestamp: ts}
	err = c.Log(action3)
	assert.NoError(t, err)
	verifyFetch(t, c, lib.ActionFetchRequest{MinActionID: 0}, []lib.Action{action1, action2, action3})

	aggregate()
	for _, cr := range requests {
		count, err := c.Count(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), count, cr)
	}
}
