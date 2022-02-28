package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http/httptest"
	"strings"
	"testing"

	action2 "fennel/controller/action"
	aggregate2 "fennel/controller/aggregate"
	"fennel/engine/ast"
	"fennel/kafka"
	"fennel/lib/aggregate"
	"fennel/model/counter"

	"fennel/client"
	"fennel/lib/action"
	"fennel/lib/ftypes"
	profilelib "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/test"

	"github.com/gorilla/mux"
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

func startTestServer(controller server) *httptest.Server {
	router := mux.NewRouter()
	controller.setHandlers(router)
	server := httptest.NewServer(router)
	return server
}

func TestLogFetchServerClient(t *testing.T) {
	// create a service
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	controller := server{tier: tier}
	server := startTestServer(controller)
	defer server.Close()
	c, err := client.NewClient(server.URL, server.Client())
	assert.NoError(t, err)

	consumer, err := tier.NewKafkaConsumer(action.ACTIONLOG_KAFKA_TOPIC, "somegroup", kafka.DefaultOffsetPolicy)
	assert.NoError(t, err)
	defer consumer.Close()

	// in the beginning, with no value set, we get []actions but with no error
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{})

	// now we add a couple of actions
	a := action.Action{ActorType: "1", ActorID: 2, ActionType: "3", TargetType: "4", TargetID: 5}
	// logging this should fail because some fields (e.g. requestID aren't specified)
	err = c.LogAction(a)
	assert.Error(t, err)
	// and no action was logged on service
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{})

	// but this error disappears when we pass all values
	a1 := add(t, c, action.Action{ActorType: "1", ActorID: 2, ActionType: "3", TargetType: "4", TargetID: 5, RequestID: 6, Timestamp: 7, Metadata: value.Nil})
	// and this action should show up in requests (after we trnasfer it to DB)
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{a1})

	// add a couple of actions
	a2 := add(t, c, action.Action{
		ActorType: "11", ActorID: 12, ActionType: "13", TargetType: "14", TargetID: 15, RequestID: 16, Timestamp: 17, Metadata: value.Nil},
	)
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{a1, a2})
	a3 := add(t, c, action.Action{
		ActorType: "22", ActorID: 23, ActionType: "23", TargetType: "24", TargetID: 25, RequestID: 26, Timestamp: 27, Metadata: value.Nil},
	)
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{a1, a2, a3})
}

// TODO: add more tests covering more error conditions
func TestProfileServerClient(t *testing.T) {
	// create a service
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	controller := server{tier: tier}
	server := startTestServer(controller)
	defer server.Close()
	c, err := client.NewClient(server.URL, server.Client())
	assert.NoError(t, err)

	// Track profiles that were set to test multi-get
	profileList := make([]profilelib.ProfileItem, 0)
	pfr := profilelib.ProfileFetchRequest{}

	// in the beginning, with no value set, we set nil pointer back but with no error
	checkGetSet(t, c, true, "1", 1, 0, "age", value.Value(nil))

	var expected value.Value = value.List([]value.Value{value.Int(1), value.Bool(false), value.Nil})
	profileList = append(profileList, checkGetSet(t, c, false, "1", 1, 1, "age", expected))
	checkGetProfileMulti(t, c, pfr, profileList)

	// we can also GetProfile it without using the specific version number
	checkGetSet(t, c, true, "1", 1, 0, "age", expected)

	// SetProfile few more key/value pairs and verify it works
	profileList = append(profileList, checkGetSet(t, c, false, "1", 1, 2, "age", value.Nil))
	checkGetProfileMulti(t, c, pfr, profileList)
	profileList = append(profileList, checkGetSet(t, c, false, "1", 3, 2, "age", value.Int(1)))
	checkGetProfileMulti(t, c, pfr, profileList)
	checkGetSet(t, c, true, "1", 1, 2, "age", value.Nil)
	checkGetSet(t, c, true, "1", 1, 0, "age", value.Nil)
	checkGetSet(t, c, false, "10", 3131, 0, "summary", value.Int(1))
}

func TestQuery(t *testing.T) {
	// create a service
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	controller := server{tier: tier}
	server := startTestServer(controller)
	defer server.Close()
	c, err := client.NewClient(server.URL, server.Client())
	assert.NoError(t, err)

	/*
		Test doesn't work because result is a value.Table which cannot be marshalled to JSON
		------------------------------------------------------------------------------------
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
		found, err := c.Query(e, value.Dict{})
		assert.NoError(t, err)
		expected := value.NewTable()
		expected.Append(map[string]value.Value{"x": value.Int(1), "y": value.Int(3)})
		expected.Append(map[string]value.Value{"x": value.Int(1), "y": value.Int(7)})
		assert.Equal(t, expected, found)
	*/
	e := ast.IfElse{
		Condition: ast.Binary{Left: ast.MakeInt(4), Op: ">", Right: ast.MakeInt(7)},
		ThenDo:    ast.MakeString("abc"),
		ElseDo:    ast.MakeString("xyz"),
	}
	found, err := c.Query(e, value.Dict{})
	assert.NoError(t, err)
	expected := value.String("xyz")
	assert.True(t, expected.Equal(found))

	// Test if dict values are set
	ast1 := ast.Var{Name: "__args__"}
	args1 := value.Dict{"key1": value.Int(4)}

	found, err = c.Query(ast1, args1)
	assert.NoError(t, err)
	assert.True(t, args1.Equal(found))
}

func TestServer_AggregateValue_Valid(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	clock := &test.FakeClock{}
	tier.Clock = clock
	t0 := ftypes.Timestamp(3600 * 10)
	clock.Set(int64(t0))
	holder := server{tier: tier}
	agg := aggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:  "count",
			Duration: 6 * 3600,
		},
	}
	key := value.Int(4)
	keystr := key.String()
	assert.Equal(t, int64(t0), tier.Clock.Now())
	assert.NoError(t, aggregate2.Store(ctx, tier, agg))
	// initially count is zero
	valueSendReceive(t, holder, agg, key, value.Int(0))

	// now create an increment
	h := counter.RollingCounter{Duration: 6 * 3600}
	t1 := t0 + 3600
	buckets := counter.BucketizeMoment(keystr, t1, value.Int(1), h.Windows())
	err = counter.Update(context.Background(), tier, agg.Name, buckets, h)
	assert.NoError(t, err)
	clock.Set(int64(t1 + 60))
	valueSendReceive(t, holder, agg, key, value.Int(1))
}

func TestServer_BatchAggregateValue(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	clock := &test.FakeClock{}
	tier.Clock = clock
	t0 := ftypes.Timestamp(0)
	holder := server{tier: tier}
	assert.Equal(t, int64(t0), tier.Clock.Now())

	agg1 := aggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:  "count",
			Duration: 6 * 3600,
		},
	}
	agg2 := aggregate.Aggregate{
		Name:      "maxelem",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:  "max",
			Duration: 6 * 3600,
		},
	}
	assert.NoError(t, aggregate2.Store(ctx, tier, agg1))
	assert.NoError(t, aggregate2.Store(ctx, tier, agg2))

	// now create changes
	t1 := t0 + 3600
	key := value.Nil
	keystr := key.String()

	h1 := counter.RollingCounter{Duration: 6 * 3600}
	buckets := counter.BucketizeMoment(keystr, t1, value.Int(1), h1.Windows())
	err = counter.Update(context.Background(), tier, agg1.Name, buckets, h1)
	assert.NoError(t, err)
	buckets = counter.BucketizeMoment(keystr, t1, value.Int(3), h1.Windows())
	err = counter.Update(context.Background(), tier, agg1.Name, buckets, h1)
	assert.NoError(t, err)
	req1 := aggregate.GetAggValueRequest{AggName: "mycounter", Key: key}

	h2 := counter.Max{Duration: 6 * 3600}
	buckets = counter.BucketizeMoment(keystr, t1, value.List{value.Int(2), value.Bool(false)}, h2.Windows())
	err = counter.Update(context.Background(), tier, agg2.Name, buckets, h2)
	assert.NoError(t, err)
	buckets = counter.BucketizeMoment(keystr, t1, value.List{value.Int(7), value.Bool(false)}, h2.Windows())
	err = counter.Update(context.Background(), tier, agg2.Name, buckets, h2)
	assert.NoError(t, err)
	req2 := aggregate.GetAggValueRequest{AggName: "maxelem", Key: key}

	clock.Set(int64(t1 + 60))
	batchValueSendReceive(t, holder,
		[]aggregate.GetAggValueRequest{req1, req2}, []value.Value{value.Int(4), value.Int(7)})
}

func TestStoreRetrieveDeactivateAggregate(t *testing.T) {
	// create a service + client
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	controller := server{tier: tier}
	server := startTestServer(controller)
	defer server.Close()
	c, err := client.NewClient(server.URL, server.Client())
	assert.NoError(t, err)

	// initially can not retrieve anything
	_, err = c.RetrieveAggregate("mycounter")
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// store a couple of aggregates
	agg := aggregate.Aggregate{
		Name:  "mycounter",
		Query: ast.MakeInt(1),
		Options: aggregate.Options{
			AggType:  "count",
			Duration: 3600 * 24,
		},
		Timestamp: 123,
	}
	err = c.StoreAggregate(agg)
	assert.NoError(t, err)
	found, err := c.RetrieveAggregate("mycounter")
	assert.NoError(t, err)
	assert.Equal(t, agg, found)
	// trying to rewrite the same agg name throws an error even if query/options are different
	agg2 := aggregate.Aggregate{
		Name:  "mycounter",
		Query: ast.MakeDouble(3.4),
		Options: aggregate.Options{
			AggType:  "count",
			Duration: 3600 * 24 * 2,
		},
		Timestamp: 123,
	}
	err = c.StoreAggregate(agg2)
	assert.Error(t, err)

	// but it works if names are different
	agg2.Name = "another counter"
	err = c.StoreAggregate(agg2)
	assert.NoError(t, err)
	found, err = c.RetrieveAggregate("another counter")
	assert.NoError(t, err)
	assert.Equal(t, agg2, found)

	// cannot retrieve after deactivating
	err = c.DeactivateAggregate(agg.Name)
	assert.NoError(t, err)
	_, err = c.RetrieveAggregate(agg.Name)
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// but can deactivate again without error
	err = c.DeactivateAggregate(agg.Name)
	assert.NoError(t, err)

	// but cannot deactivate aggregate that does not exist
	err = c.DeactivateAggregate("nonexistent aggregate")
	assert.Error(t, err)
}

func checkGetSet(t *testing.T, c *client.Client, get bool, otype string, oid uint64, version uint64,
	key string, val value.Value) profilelib.ProfileItem {
	if get {
		req := profilelib.NewProfileItem(otype, oid, key, version)
		found, err := c.GetProfile(&req)
		assert.NoError(t, err)
		if found == nil {
			assert.Equal(t, nil, val)
		} else {
			assert.Equal(t, val, *found)
		}
		return req
	} else {
		profile := profilelib.ProfileItem{OType: ftypes.OType(otype), Oid: oid, Key: key, Version: version, Value: val}
		err := c.SetProfile(&profile)
		assert.NoError(t, err)
		request := profilelib.NewProfileItem(otype, oid, key, version)
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

func checkGetProfileMulti(t *testing.T, c *client.Client, request profilelib.ProfileFetchRequest, expected []profilelib.ProfileItem) {
	found, err := c.GetProfileMulti(request)
	assert.NoError(t, err)

	assert.Equal(t, len(expected), len(found))
	for i := range expected {
		assert.Equal(t, expected[i], found[i])
	}
}

func valueSendReceive(t *testing.T, controller server, agg aggregate.Aggregate, key, expected value.Value) {
	gavr := aggregate.GetAggValueRequest{AggName: agg.Name, Key: key}
	ser, err := json.Marshal(gavr)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/aggregate_value", strings.NewReader(string(ser)))
	controller.AggregateValue(w, r)
	// parse server response back
	response, err := ioutil.ReadAll(w.Body)
	assert.NoError(t, err)
	found, err := value.FromJSON(response)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}

func batchValueSendReceive(t *testing.T, controller server,
	reqs []aggregate.GetAggValueRequest, expectedVals []value.Value) {
	ser, err := json.Marshal(reqs)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/batch_aggregate_value", strings.NewReader(string(ser)))
	controller.BatchAggregateValue(w, r)
	// convert vals to json and compare
	found, err := ioutil.ReadAll(w.Body)
	assert.NoError(t, err)
	expected, err := json.Marshal(expectedVals)
	assert.Equal(t, expected, found)
}
