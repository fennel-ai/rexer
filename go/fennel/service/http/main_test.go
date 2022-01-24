package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"fennel/client"
	"fennel/lib/action"
	counterlib "fennel/lib/counter"
	"fennel/lib/ftypes"
	profilelib "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/model/counter"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func verifyFetch(t *testing.T, c *client.Client, request action.ActionFetchRequest, expected []action.Action) {
	found, err := c.FetchActions(request)
	assert.NoError(t, err)
	equals(t, expected, found)
}

func equals(t *testing.T, expected []action.Action, found []action.Action) {
	// we don't test equality of found/expected directly
	// action_ids aren't SetProfile properly in expected yet
	// instead, we use Equals method on Action struct and tell it to ignore IDs
	assert.Equal(t, len(expected), len(found))
	for i, e := range expected {
		assert.True(t, e.Equals(found[i], true))
	}
}

func add(t *testing.T, c *client.Client, a action.Action) action.Action {
	err := c.LogAction(a)
	assert.NoError(t, err)
	return a
}

func startTestServer(controller holder) *httptest.Server {
	mux := http.NewServeMux()
	setHandlers(controller, mux)
	server := httptest.NewServer(mux)
	return server
}

func TestLogFetchServerClient(t *testing.T) {
	// create a service
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)
	controller := holder{instance: instance}
	server := startTestServer(controller)
	defer server.Close()
	c, err := client.NewClient(server.URL, server.Client())
	assert.NoError(t, err)

	// in the beginning, with no value SetProfile, we GetProfile []actions but with no error
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{})

	// now we add a couple of actions
	a := action.Action{ActorType: 1, ActorID: 2, ActionType: 3, TargetType: 4, TargetID: 5}
	// logging this should fail because some fields (e.g. requestID aren't specified)
	err = c.LogAction(a)
	assert.Error(t, err)
	// and no action was logged on service
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{})

	// but this error disappears when we pass all values
	action1 := add(t, c, action.Action{ActorType: 1, ActorID: 2, ActionType: 3, TargetType: 4, TargetID: 5, RequestID: 6, Timestamp: 7})
	// and this action should show up in requests
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{action1})

	// add a couple of actions
	action2 := add(t, c, action.Action{
		ActorType: 11, ActorID: 12, ActionType: 13, TargetType: 14, TargetID: 15, RequestID: 16, Timestamp: 17},
	)
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{action1, action2})
	action3 := add(t, c, action.Action{
		ActorType: 22, ActorID: 23, ActionType: 23, TargetType: 24, TargetID: 25, RequestID: 26, Timestamp: 27},
	)
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{action1, action2, action3})
}

// TODO: add more tests covering more error conditions
func TestProfileServerClient(t *testing.T) {
	// create a service
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)
	controller := holder{instance: instance}
	server := startTestServer(controller)
	defer server.Close()
	c, err := client.NewClient(server.URL, server.Client())
	assert.NoError(t, err)

	// in the beginning, with no value SetProfile, we GetProfile nil pointer back but with no error
	checkGetSet(t, c, true, 1, 1, 1, 0, "age", value.Value(nil))

	var expected value.Value = value.List([]value.Value{value.Int(1), value.Bool(false), value.Nil})
	checkGetSet(t, c, false, 1, 1, 1, 1, "age", expected)

	// we can also GetProfile it without using the specific version number
	checkGetSet(t, c, true, 1, 1, 1, 0, "age", expected)

	// SetProfile few more key/value pairs and verify it works
	checkGetSet(t, c, false, 1, 1, 1, 2, "age", value.Nil)
	checkGetSet(t, c, false, 1, 1, 3, 2, "age", value.Int(1))
	checkGetSet(t, c, true, 1, 1, 1, 2, "age", value.Nil)
	checkGetSet(t, c, true, 1, 1, 1, 0, "age", value.Nil)
	checkGetSet(t, c, false, 1, 10, 3131, 0, "summary", value.Int(1))
}

func TestCountRateServerClient(t *testing.T) {
	// create a service
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)
	controller := holder{instance: instance}
	server := startTestServer(controller)
	defer server.Close()
	c, err := client.NewClient(server.URL, server.Client())
	assert.NoError(t, err)

	uid := ftypes.OidType(1)
	video_id := ftypes.OidType(2)
	ts := ftypes.Timestamp(123)
	cr := counterlib.GetCountRequest{CounterType: counterlib.CounterType_USER_LIKE, Window: ftypes.Window_HOUR, Key: ftypes.Key{uid}, Timestamp: ts}
	rr := counterlib.GetRateRequest{
		counterlib.CounterType_USER_LIKE,
		counterlib.CounterType_VIDEO_LIKE,
		ftypes.Key{uid},
		ftypes.Key{video_id},
		ftypes.Window_HOUR,
		ts,
		true,
	}

	// initially counts & rates are zero
	count, err := c.GetCount(cr)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), count)
	rate, err := c.GetRate(rr)
	assert.NoError(t, err)
	assert.Equal(t, float64(0), rate)

	// increment a couple of keys via counter controller
	counter.Increment(instance, cr.CounterType, cr.Window, cr.Key, cr.Timestamp, 35)
	counter.Increment(instance, rr.DenCounterType, cr.Window, ftypes.Key{video_id}, cr.Timestamp, 200)
	count, err = c.GetCount(cr)
	assert.NoError(t, err)
	assert.Equal(t, uint64(35), count)

	rate, err = c.GetRate(rr)
	assert.NoError(t, err)
	assert.Equal(t, 0.128604412, rate)

	// but if *somehow* numerator was bigger than denominator, getRate should give an error (because wilson isn't defined)
	counter.Increment(instance, cr.CounterType, cr.Window, cr.Key, cr.Timestamp, 1000)
	_, err = c.GetRate(rr)
	assert.Error(t, err)
}

func checkGetSet(t *testing.T, c *client.Client, get bool, custid uint64, otype uint32, oid uint64, version uint64,
	key string, val value.Value) {
	if get {
		req := profilelib.NewProfileItem(custid, otype, oid, key, version)
		found, err := c.GetProfile(&req)
		assert.NoError(t, err)
		if found == nil {
			assert.Equal(t, nil, val)
		} else {
			assert.Equal(t, val, *found)
		}
	} else {
		err := c.SetProfile(&profilelib.ProfileItem{CustID: ftypes.CustID(custid), OType: otype, Oid: oid, Key: key, Version: version, Value: val})
		assert.NoError(t, err)
		request := profilelib.NewProfileItem(custid, otype, oid, key, version)
		found, err := c.GetProfile(&request)
		assert.NoError(t, err)
		if found == nil {
			assert.Equal(t, nil, val)
		} else {
			assert.Equal(t, val, *found)
		}
	}
}
