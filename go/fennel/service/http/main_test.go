package main

import (
	"fennel/engine/ast"
	"fennel/lib/aggregate"
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
	// action_ids aren't set properly in expected yet
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

	// in the beginning, with no value set, we get []actions but with no error
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{})

	// now we add a couple of actions
	a := action.Action{CustID: 1, ActorType: 1, ActorID: 2, ActionType: 3, TargetType: 4, TargetID: 5}
	// logging this should fail because some fields (e.g. requestID aren't specified)
	err = c.LogAction(a)
	assert.Error(t, err)
	// and no action was logged on service
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{})

	// but this error disappears when we pass all values
	action1 := add(t, c, action.Action{CustID: 1, ActorType: 1, ActorID: 2, ActionType: 3, TargetType: 4, TargetID: 5, RequestID: 6, Timestamp: 7})
	// and this action should show up in requests
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{action1})

	// add a couple of actions
	action2 := add(t, c, action.Action{
		CustID: 1, ActorType: 11, ActorID: 12, ActionType: 13, TargetType: 14, TargetID: 15, RequestID: 16, Timestamp: 17},
	)
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{action1, action2})
	action3 := add(t, c, action.Action{
		CustID: 1, ActorType: 22, ActorID: 23, ActionType: 23, TargetType: 24, TargetID: 25, RequestID: 26, Timestamp: 27},
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

	// Track profiles that were set to test multi-get
	profileList := make([]profilelib.ProfileItem, 0)
	pfr := profilelib.ProfileFetchRequest{}

	// in the beginning, with no value SetProfile, we GetProfile nil pointer back but with no error
	checkGetSet(t, c, true, 1, 1, 1, 0, "age", value.Value(nil))

	var expected value.Value = value.List([]value.Value{value.Int(1), value.Bool(false), value.Nil})
	profileList = append(profileList, checkGetSet(t, c, false, 1, 1, 1, 1, "age", expected))
	checkGetProfiles(t, c, pfr, profileList)

	// we can also GetProfile it without using the specific version number
	checkGetSet(t, c, true, 1, 1, 1, 0, "age", expected)

	// SetProfile few more key/value pairs and verify it works
	profileList = append(profileList, checkGetSet(t, c, false, 1, 1, 1, 2, "age", value.Nil))
	checkGetProfiles(t, c, pfr, profileList)
	profileList = append(profileList, checkGetSet(t, c, false, 1, 1, 3, 2, "age", value.Int(1)))
	checkGetProfiles(t, c, pfr, profileList)
	checkGetSet(t, c, true, 1, 1, 1, 2, "age", value.Nil)
	checkGetSet(t, c, true, 1, 1, 1, 0, "age", value.Nil)
	profileList = append(profileList, checkGetSet(t, c, false, 1, 10, 3131, 0, "summary", value.Int(1)))
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

func TestQuery(t *testing.T) {
	// create a service
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)
	controller := holder{instance: instance}
	server := startTestServer(controller)
	defer server.Close()
	c, err := client.NewClient(server.URL, server.Client())
	assert.NoError(t, err)

	d1 := ast.Dict{Values: map[string]ast.Ast{"x": ast.MakeInt(1), "y": ast.MakeInt(3)}}
	d2 := ast.Dict{Values: map[string]ast.Ast{"x": ast.MakeInt(3), "y": ast.MakeInt(4)}}
	d3 := ast.Dict{Values: map[string]ast.Ast{"x": ast.MakeInt(1), "y": ast.MakeInt(7)}}
	table := ast.Table{Inner: ast.List{Values: []ast.Ast{d1, d2, d3}}}
	e := ast.OpCall{
		Operand:   table,
		Namespace: "std",
		Name:      "filter",
		Kwargs: ast.Dict{map[string]ast.Ast{"where": ast.Binary{
			Left: ast.Lookup{On: ast.At{}, Property: "x"},
			Op:   "<",
			Right: ast.Binary{
				Left:  ast.Lookup{On: ast.At{}, Property: "y"},
				Op:    "-",
				Right: ast.MakeInt(1),
			},
		}},
		},
	}
	found, err := c.Query(e)
	assert.NoError(t, err)
	expected := value.NewTable()
	expected.Append(map[string]value.Value{"x": value.Int(1), "y": value.Int(3)})
	expected.Append(map[string]value.Value{"x": value.Int(1), "y": value.Int(7)})
	assert.Equal(t, expected, found)

}

func TestStoreRetrieveAggregate(t *testing.T) {
	// create a service + client
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)
	controller := holder{instance: instance}
	server := startTestServer(controller)
	defer server.Close()
	c, err := client.NewClient(server.URL, server.Client())
	assert.NoError(t, err)

	// initially can not retrieve anything
	_, err = c.RetrieveAggregate("counter", "mycounter")
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// store a couple of aggregates
	agg := aggregate.Aggregate{
		Type:  "counter",
		Name:  "mycounter",
		Query: ast.MakeInt(1),
		Options: aggregate.AggOptions{
			WindowType: aggregate.WindowType_LAST,
			Duration:   3600 * 24,
			Retention:  3600 * 24 * 7,
		},
	}
	err = c.StoreAggregate(agg)
	assert.NoError(t, err)
	found, err := c.RetrieveAggregate("counter", "mycounter")
	assert.NoError(t, err)
	assert.Equal(t, agg, found)
	// trying to rewrite the same agg type/name throws an error even if query/options are different
	agg2 := aggregate.Aggregate{
		Type:  "counter",
		Name:  "mycounter",
		Query: ast.MakeDouble(3.4),
		Options: aggregate.AggOptions{
			WindowType: aggregate.WindowType_TIMESERIES,
			Duration:   3600 * 24,
			Retention:  3600 * 24 * 7,
		},
	}
	err = c.StoreAggregate(agg2)
	assert.Error(t, err)

	// but it works if even one of type/name are different
	agg2.Name = "another counter"
	err = c.StoreAggregate(agg2)
	assert.NoError(t, err)
	found, err = c.RetrieveAggregate("counter", "another counter")
	assert.NoError(t, err)
	assert.Equal(t, agg2, found)

}
func checkGetSet(t *testing.T, c *client.Client, get bool, custid uint64, otype uint32, oid uint64, version uint64,
	key string, val value.Value) profilelib.ProfileItem {
	if get {
		req := profilelib.NewProfileItem(custid, otype, oid, key, version)
		found, err := c.GetProfile(&req)
		assert.NoError(t, err)
		if found == nil {
			assert.Equal(t, nil, val)
		} else {
			assert.Equal(t, val, *found)
		}
		return req
	} else {
		profile := profilelib.ProfileItem{CustID: ftypes.CustID(custid), OType: otype, Oid: oid, Key: key, Version: version, Value: val}
		err := c.SetProfile(&profile)
		assert.NoError(t, err)
		request := profilelib.NewProfileItem(custid, otype, oid, key, version)
		found, err := c.GetProfile(&request)
		assert.NoError(t, err)
		if found == nil {
			assert.Equal(t, nil, val)
		} else {
			assert.Equal(t, val, *found)
		}
		return profile
	}
}

func checkGetProfiles(t *testing.T, c *client.Client, request profilelib.ProfileFetchRequest, expected []profilelib.ProfileItem) {
	found, err := c.GetProfiles(request)
	assert.NoError(t, err)

	assert.Equal(t, len(expected), len(found))
	for i := range expected {
		assert.Equal(t, expected[i], found[i])
	}
}
