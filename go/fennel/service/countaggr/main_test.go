package main

import (
	"fennel/controller/action"
	"fennel/controller/counter"
	"fennel/instance"
	actionlib "fennel/lib/action"
	counterlib "fennel/lib/counter"
	"fennel/lib/ftypes"
	profileLib "fennel/lib/profile"
	"fennel/test"
	"testing"

	"github.com/stretchr/testify/assert"
)

func add(instance instance.Instance, t *testing.T, a actionlib.Action) actionlib.Action {
	aid, err := action.Insert(instance, a)
	assert.NoError(t, err)
	a.ActionID = ftypes.OidType(aid)
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
	assert.NoError(t, err)

	// start the service
	//go serve(controller)
	//defer shutDownServer()
	// and create a client
	//c := client.NewClient("http://localhost")

	// Initially count for keys are zero
	uid := ftypes.OidType(1)
	video_id := ftypes.OidType(2)
	ts := ftypes.Timestamp(123)

	requests := []counterlib.GetCountRequest{
		{counterlib.CounterType_USER_LIKE, ftypes.Window_HOUR, ftypes.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, ftypes.Window_DAY, ftypes.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, ftypes.Window_WEEK, ftypes.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, ftypes.Window_MONTH, ftypes.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, ftypes.Window_QUARTER, ftypes.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, ftypes.Window_YEAR, ftypes.Key{uid}, ts},
		{counterlib.CounterType_USER_LIKE, ftypes.Window_FOREVER, ftypes.Key{uid}, ts},
		{counterlib.CounterType_VIDEO_LIKE, ftypes.Window_HOUR, ftypes.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, ftypes.Window_DAY, ftypes.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, ftypes.Window_WEEK, ftypes.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, ftypes.Window_MONTH, ftypes.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, ftypes.Window_QUARTER, ftypes.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, ftypes.Window_YEAR, ftypes.Key{video_id}, ts},
		{counterlib.CounterType_VIDEO_LIKE, ftypes.Window_FOREVER, ftypes.Key{video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, ftypes.Window_HOUR, ftypes.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, ftypes.Window_DAY, ftypes.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, ftypes.Window_WEEK, ftypes.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, ftypes.Window_MONTH, ftypes.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, ftypes.Window_QUARTER, ftypes.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, ftypes.Window_YEAR, ftypes.Key{uid, video_id}, ts},
		{counterlib.CounterType_USER_VIDEO_LIKE, ftypes.Window_FOREVER, ftypes.Key{uid, video_id}, ts},
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
		ftypes.Key{uid},
		ftypes.Key{video_id},
		ftypes.Window_HOUR,
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
