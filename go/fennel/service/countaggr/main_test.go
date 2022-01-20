package main

import (
	"fennel/controller/action"
	"fennel/controller/counter"
	"fennel/instance"
	actionlib "fennel/lib/action"
	counterlib "fennel/lib/counter"
	profileLib "fennel/lib/profile"
	"fennel/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func add(instance instance.Instance, t *testing.T, a actionlib.Action) actionlib.Action {
	aid, err := action.Insert(instance, a)
	assert.NoError(t, err)
	a.ActionID = profileLib.OidType(aid)
	return a
}

func verifyFetch(instance instance.Instance, t *testing.T, request actionlib.ActionFetchRequest, expected []actionlib.Action) {
	found, err := action.Fetch(instance, request)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
	//equals(t, expected, found)
}

func TestEndToEnd(t *testing.T) {
	instance, err := test.DefaultInstance()
	//controller, err := DefaultMainController()
	assert.NoError(t, err)

	// start the service
	//go serve(controller)
	//defer shutDownServer()
	// and create a client
	//c := client.NewClient("http://localhost")

	// Initially count for keys are zero
	uid := profileLib.OidType(1)
	video_id := profileLib.OidType(2)
	ts := actionlib.Timestamp(123)

	requests := []counterlib.GetCountRequest{
		{counterlib.CounterType_USER_LIKE, counterlib.Window_HOUR, counterlib.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, counterlib.Window_DAY, counterlib.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, counterlib.Window_WEEK, counterlib.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, counterlib.Window_MONTH, counterlib.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, counterlib.Window_QUARTER, counterlib.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, counterlib.Window_YEAR, counterlib.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, counterlib.Window_FOREVER, counterlib.Key{uid}, ts},
		{counterlib.CounterType_VIDEO_LIKE, counterlib.Window_HOUR, counterlib.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, counterlib.Window_DAY, counterlib.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, counterlib.Window_WEEK, counterlib.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, counterlib.Window_MONTH, counterlib.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, counterlib.Window_QUARTER, counterlib.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, counterlib.Window_YEAR, counterlib.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, counterlib.Window_FOREVER, counterlib.Key{video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, counterlib.Window_HOUR, counterlib.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, counterlib.Window_DAY, counterlib.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, counterlib.Window_WEEK, counterlib.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, counterlib.Window_MONTH, counterlib.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, counterlib.Window_QUARTER, counterlib.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, counterlib.Window_YEAR, counterlib.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, counterlib.Window_FOREVER, counterlib.Key{uid, video_id}, ts},
	}
	for _, cr := range requests {
		count, err := counter.Count(instance, cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), count)
	}

	// now make an event
	action1 := actionlib.Action{ActorType: profileLib.User, ActorID: uid, TargetType: profileLib.Video, TargetID: video_id, ActionType: actionlib.Like, RequestID: 1, Timestamp: ts}
	action1 = add(instance, t, action1)

	// and verify it went through
	verifyFetch(instance, t, actionlib.ActionFetchRequest{MinActionID: 0}, []actionlib.Action{action1})

	// and all counts should still be zero
	for _, cr := range requests {
		count, err := counter.Count(instance, cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), count)
	}
	// but counts should go up after we aggregate
	aggregate(instance)
	for _, cr := range requests {
		count, err := counter.Count(instance, cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count, cr)
	}
	// now make one more event which is not of matching actionlib type (Like vs Share)
	action2 := actionlib.Action{ActorType: profileLib.User, ActorID: uid, TargetType: profileLib.Video, TargetID: video_id, ActionType: actionlib.Share, RequestID: 1, Timestamp: ts}
	action2 = add(instance, t, action2)
	verifyFetch(instance, t, actionlib.ActionFetchRequest{MinActionID: 0}, []actionlib.Action{action1, action2})

	// this one doesn't change the counts because actionlib type doesn't match
	aggregate(instance)
	for _, cr := range requests {
		count, err := counter.Count(instance, cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), count, cr)
	}

	// but another valid event will make the counts go up
	action3 := actionlib.Action{ActorType: profileLib.User, ActorID: uid, TargetType: profileLib.Video, TargetID: video_id, ActionType: actionlib.Like, RequestID: 1, Timestamp: ts}
	action3 = add(instance, t, action3)
	verifyFetch(instance, t, actionlib.ActionFetchRequest{MinActionID: 0}, []actionlib.Action{action1, action2, action3})

	aggregate(instance)
	for _, cr := range requests {
		count, err := counter.Count(instance, cr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), count, cr)
	}

	// and finally we can also do rate checks
	rr := counterlib.GetRateRequest{
		counterlib.CounterType_USER_LIKE,
		counterlib.CounterType_VIDEO_LIKE,
		counterlib.Key{uid},
		counterlib.Key{video_id},
		counterlib.Window_HOUR,
		ts,
		true,
	}
	rate, err := counter.Rate(instance, rr)
	assert.NoError(t, err)
	assert.Equal(t, 0.3423719528896193, rate)
	// and verify upper bound too
	rr.LowerBound = false
	rate, err = counter.Rate(instance, rr)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, rate)
}
