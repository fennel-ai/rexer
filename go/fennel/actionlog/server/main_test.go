package main

import (
	"fennel/actionlog/client"
	"fennel/actionlog/lib"
	"github.com/stretchr/testify/assert"
	"testing"
)

func check(t *testing.T, c client.Client, request lib.ActionFetchRequest, expected []lib.Action) {
	found, err := c.Fetch(request)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}

func add(t *testing.T, c client.Client, a lib.Action) lib.Action {
	aid, err := c.Log(a)
	assert.NoError(t, err)
	a.ActionID = aid
	return a
}

func TestServerClientBasic(t *testing.T) {
	t.Cleanup(dbInit)
	// start the server
	go serve()
	// and create a client
	c := client.NewClient("http://localhost")

	// in the beginning, with no value set, we get []actions but with no error
	check(t, c, lib.ActionFetchRequest{}, []lib.Action{})

	// now we add a couple of actions
	action := lib.Action{ActorType: 1, ActorID: 2, ActionType: 3, TargetType: 4, TargetID: 5}
	// logging this should fail because some fields (e.g. requestID aren't specified)
	_, err := c.Log(action)
	assert.Error(t, err)
	// and no action was logged on server
	check(t, c, lib.ActionFetchRequest{}, []lib.Action{})

	// but this error disappears when we pass all values
	action1 := add(t, c, lib.Action{
		ActorType: 1, ActorID: 2, ActionType: 3, TargetType: 4, TargetID: 5, RequestID: 6, Timestamp: 7},
	)
	// and this action should show up in requests
	check(t, c, lib.ActionFetchRequest{}, []lib.Action{action1})

	// add a couple of actions
	action2 := add(t, c, lib.Action{
		ActorType: 11, ActorID: 12, ActionType: 13, TargetType: 14, TargetID: 15, RequestID: 16, Timestamp: 17},
	)
	check(t, c, lib.ActionFetchRequest{}, []lib.Action{action1, action2})
	action3 := add(t, c, lib.Action{
		ActorType: 22, ActorID: 23, ActionType: 23, TargetType: 24, TargetID: 25, RequestID: 26, Timestamp: 27},
	)
	check(t, c, lib.ActionFetchRequest{}, []lib.Action{action1, action2, action3})

	// check with actionID
	check(t, c, lib.ActionFetchRequest{MinActionID: action2.ActionID}, []lib.Action{action3})
	check(t, c, lib.ActionFetchRequest{MinActionID: action1.ActionID, MaxActionID: action2.ActionID}, []lib.Action{action2})
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
	aid, err := c.Log(action1)
	assert.NoError(t, err)
	action1.ActionID = aid

	// and verify it went through
	found, err := c.Fetch(lib.ActionFetchRequest{MinActionID: 0})
	assert.NoError(t, err)
	assert.Equal(t, []lib.Action{action1}, found)

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
	aid, err = c.Log(action2)
	assert.NoError(t, err)
	action2.ActionID = aid
	found, err = c.Fetch(lib.ActionFetchRequest{MinActionID: 0})
	assert.NoError(t, err)
	assert.Equal(t, []lib.Action{action1, action2}, found)
	// this one doesn't change the counts because action type doesn't match
	aggregate()
	for _, cr := range requests {
		count, err := c.Count(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count, cr)
	}

	// but another valid event will make the counts go up
	action3 := lib.Action{ActorType: lib.User, ActorID: uid, TargetType: lib.Video, TargetID: video_id, ActionType: lib.Like, RequestID: 1, Timestamp: ts}
	aid, err = c.Log(action3)
	assert.NoError(t, err)
	action3.ActionID = aid
	found, err = c.Fetch(lib.ActionFetchRequest{MinActionID: 0})
	assert.NoError(t, err)
	assert.Equal(t, []lib.Action{action1, action2, action3}, found)

	aggregate()
	for _, cr := range requests {
		count, err := c.Count(cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), count, cr)
	}
}
