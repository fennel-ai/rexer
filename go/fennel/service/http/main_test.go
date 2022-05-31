package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	action2 "fennel/controller/action"
	aggregate2 "fennel/controller/aggregate"
	"fennel/controller/mock"
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
	"google.golang.org/protobuf/proto"
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

// add logs an action without a dedup key
func add(t *testing.T, c *client.Client, a action.Action) {
	err := c.LogAction(a, "")
	assert.NoError(t, err)
}

// addBatch logs multiple actions with no dedup key for any of them
func addBatch(t *testing.T, c *client.Client, as []action.Action) {
	err := c.LogActions(as, nil)
	assert.NoError(t, err)
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

	consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        action.ACTIONLOG_KAFKA_TOPIC,
		GroupID:      "somegroup",
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	assert.NoError(t, err)
	defer consumer.Close()

	// in the beginning, with no value set, we get []actions but with no error
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{})

	// now we add a couple of actions
	a := action.Action{ActorType: "1", ActorID: "2", ActionType: "3", TargetType: "4", TargetID: "5"}
	// logging this should fail because some fields (e.g. requestID aren't specified)
	err = c.LogAction(a, "")
	assert.Error(t, err)
	// and no action was logged on service
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{})

	// but this error disappears when we pass all values
	a1 := action.Action{ActorType: "1", ActorID: "2", ActionType: "3", TargetType: "4", TargetID: "5", RequestID: "6", Timestamp: 7, Metadata: value.Nil}
	add(t, c, a1)
	// and this action should show up in requests (after we transfer it to DB)
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{a1})

	// add a couple of actions as a batch
	a2 := action.Action{
		ActorType: "11", ActorID: "12", ActionType: "13", TargetType: "14", TargetID: "15", RequestID: "16", Timestamp: 17, Metadata: value.Nil}
	a3 := action.Action{
		ActorType: "22", ActorID: "23", ActionType: "23", TargetType: "24", TargetID: "25", RequestID: "26", Timestamp: 27, Metadata: value.Nil}
	addBatch(t, c, []action.Action{a2, a3})
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	verifyFetch(t, c, action.ActionFetchRequest{}, []action.Action{a1, a2, a3})

	// test duplicate behaviour without dedup_key
	d1 := action.Action{
		ActorID:    "1",
		ActorType:  "2",
		TargetID:   "3",
		TargetType: "4",
		ActionType: "no_dedup",
		Timestamp:  5,
		RequestID:  "6",
		Metadata:   value.Nil,
	}
	err = c.LogAction(d1, "")
	assert.NoError(t, err)
	err = c.LogAction(d1, "")
	assert.NoError(t, err)
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	// two actions without dedup keys, should get back two actions
	verifyFetch(t, c, action.ActionFetchRequest{ActionType: "no_dedup"}, []action.Action{d1, d1})

	// test duplicate behaviour with dedup_key
	d2 := action.Action{
		ActorID:    "1",
		ActorType:  "2",
		TargetID:   "3",
		TargetType: "4",
		ActionType: "dedup",
		Timestamp:  5,
		RequestID:  "6",
		Metadata:   value.Nil,
	}
	err = c.LogAction(d2, "dedup_key")
	assert.NoError(t, err)
	err = c.LogAction(d2, "dedup_key")
	assert.NoError(t, err)
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	// logged two actions with same dedup key, should get back one action
	verifyFetch(t, c, action.ActionFetchRequest{ActionType: "dedup"}, []action.Action{d2})

	// now test duplicates with log_multi
	d3 := action.Action{
		ActorID:    "1",
		ActorType:  "2",
		TargetID:   "3",
		TargetType: "4",
		ActionType: "no_dedup_multi",
		Timestamp:  5,
		RequestID:  "6",
		Metadata:   value.Nil,
	}
	err = c.LogActions([]action.Action{d3, d3, d3}, nil)
	assert.NoError(t, err)
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	// logged three actions with no dedup key, should get back three actions
	verifyFetch(t, c, action.ActionFetchRequest{ActionType: "no_dedup_multi"}, []action.Action{d3, d3, d3})

	d4 := action.Action{
		ActorID:    "1",
		ActorType:  "2",
		TargetID:   "3",
		TargetType: "4",
		ActionType: "dedup_multi",
		Timestamp:  5,
		RequestID:  "6",
		Metadata:   value.Nil,
	}
	err = c.LogActions([]action.Action{d4, d4, d4}, []string{"dedup_multi", "dedup_multi", "dedup_multi"})
	assert.NoError(t, err)
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	// logged three actions with same dedup key, should get back one action
	verifyFetch(t, c, action.ActionFetchRequest{ActionType: "dedup_multi"}, []action.Action{d4})

	// now test with a mix of dedup keys
	d5 := action.Action{
		ActorID:    "1",
		ActorType:  "2",
		TargetID:   "3",
		TargetType: "4",
		ActionType: "dedup_mix",
		Timestamp:  5,
		RequestID:  "6",
		Metadata:   value.Nil,
	}
	err = c.LogActions([]action.Action{d5, d5, d5, d5, d5},
		[]string{"dedup_mix_1", "", "dedup_mix_2", "dedup_mix_2", "dedup_mix_1"})
	assert.NoError(t, err)
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	// of the 5 actions, only three of them will be set
	verifyFetch(t, c, action.ActionFetchRequest{ActionType: "dedup_mix"}, []action.Action{d5, d5, d5})
}

func TestActionDedupedPerActionType(t *testing.T) {
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

	consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        action.ACTIONLOG_KAFKA_TOPIC,
		GroupID:      "somegroup",
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	assert.NoError(t, err)
	defer consumer.Close()

	// same dedup key with same action type should be deduped
	f1 := action.Action{
		ActorID:    "10",
		ActorType:  "2",
		TargetID:   "3",
		TargetType: "4",
		ActionType: "at1",
		Timestamp:  5,
		RequestID:  "6",
		Metadata:   value.Nil,
	}
	f2 := action.Action{
		ActorID:    "11",
		ActorType:  "2",
		TargetID:   "3",
		TargetType: "4",
		ActionType: "at1",
		Timestamp:  5,
		RequestID:  "6",
		Metadata:   value.Nil,
	}
	err = c.LogAction(f1, "dedup_key")
	assert.NoError(t, err)
	err = c.LogAction(f2, "dedup_key")
	assert.NoError(t, err)
	// only the first action is logged
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	verifyFetch(t, c, action.ActionFetchRequest{ActionType: "at1"}, []action.Action{f1})

	// same dedup key but for different action type should log the actions
	a1 := action.Action{
		ActorID:    "1",
		ActorType:  "2",
		TargetID:   "3",
		TargetType: "4",
		ActionType: "at1",
		Timestamp:  5,
		RequestID:  "6",
		Metadata:   value.Nil,
	}

	a2 := action.Action{
		ActorID:    "1",
		ActorType:  "2",
		TargetID:   "3",
		TargetType: "4",
		ActionType: "at2",
		Timestamp:  5,
		RequestID:  "6",
		Metadata:   value.Nil,
	}

	assert.NoError(t, c.LogActions([]action.Action{a1, a2}, []string{"same_key", "same_key"}))
	assert.NoError(t, action2.TransferToDB(ctx, tier, consumer))
	verifyFetch(t, c, action.ActionFetchRequest{ActorID: "1"}, []action.Action{a1, a2})
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

	var expected value.Value = value.NewList(value.Int(1), value.Bool(false), value.Nil)
	profileList = append(profileList, checkSet(t, c, "1", "1", 1, "age", expected))

	profileList = append(profileList, checkSet(t, c, "1", "2", 2, "age", value.Nil))
	profileList = append(profileList, checkSet(t, c, "1", "3", 3, "age", value.Int(1)))

	checkSet(t, c, "10", "3131", 4, "summary", value.Int(1))

	// these profiles are also written to kafka queue
	consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        profilelib.PROFILELOG_KAFKA_TOPIC,
		GroupID:      "someprofilegroup",
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	assert.NoError(t, err)
	actual, err := batchReadProfilesFromConsumer(t, context.Background(), consumer, 3)
	assert.NoError(t, err)
	assert.ElementsMatch(t, profileList, actual)
	consumer.Close()
}

func TestSetProfilesQueuesToKafka(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	controller := server{tier: tier}
	server := startTestServer(controller)
	defer server.Close()
	c, err := client.NewClient(server.URL, server.Client())
	assert.NoError(t, err)

	// Write another batch, previous entries should still be there
	profileList2 := make([]profilelib.ProfileItem, 0)
	for i := uint64(1); i <= 3; i++ {
		p := profilelib.ProfileItem{OType: ftypes.OType("2"), Oid: fmt.Sprint(i), Key: "foo", UpdateTime: i, Value: value.Int(i * 10)}
		profileList2 = append(profileList2, p)
	}

	assert.NoError(t, c.SetProfiles(profileList2))

	consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        profilelib.PROFILELOG_KAFKA_TOPIC,
		GroupID:      "someprofilegroup2",
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	assert.NoError(t, err)
	actual, err := batchReadProfilesFromConsumer(t, context.Background(), consumer, 6)
	assert.NoError(t, err)
	assert.ElementsMatch(t, profileList2, actual)
	consumer.Close()
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

	d1 := ast.Dict{Values: map[string]ast.Ast{"x": ast.MakeInt(1), "y": ast.MakeInt(3)}}
	d2 := ast.Dict{Values: map[string]ast.Ast{"x": ast.MakeInt(3), "y": ast.MakeInt(4)}}
	d3 := ast.Dict{Values: map[string]ast.Ast{"x": ast.MakeInt(1), "y": ast.MakeInt(7)}}
	table := ast.List{Values: []ast.Ast{d1, d2, d3}}
	e := ast.OpCall{
		Operands:  []ast.Ast{table},
		Vars:      []string{"at"},
		Namespace: "std",
		Name:      "filter",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{"where": ast.Binary{
			Left: ast.Lookup{On: ast.Var{Name: "at"}, Property: "x"},
			Op:   "<",
			Right: ast.Binary{
				Left:  ast.Lookup{On: ast.Var{Name: "at"}, Property: "y"},
				Op:    "-",
				Right: ast.Var{Name: "c"},
			},
		}},
		},
	}
	found, err := c.Query(e, value.NewDict(map[string]value.Value{"c": value.Int(1)}), mock.Data{})
	assert.NoError(t, err)
	expected := value.NewList()
	expected.Append(value.NewDict(map[string]value.Value{"x": value.Int(1), "y": value.Int(3)}))
	expected.Append(value.NewDict(map[string]value.Value{"x": value.Int(1), "y": value.Int(7)}))
	assert.Equal(t, expected, found)
	e2 := ast.IfElse{
		Condition: ast.Binary{Left: ast.MakeInt(4), Op: ">", Right: ast.MakeInt(7)},
		ThenDo:    ast.MakeString("abc"),
		ElseDo:    ast.MakeString("xyz"),
	}
	found, err = c.Query(e2, value.NewDict(nil), mock.Data{})
	assert.NoError(t, err)
	assert.True(t, value.String("xyz").Equal(found))

	// Test if dict values are set
	ast1 := ast.Var{Name: "key1"}
	args1 := value.NewDict(map[string]value.Value{"key1": value.Int(4)})
	exp1 := value.Int(4)

	found, err = c.Query(ast1, args1, mock.Data{})
	assert.NoError(t, err)
	assert.True(t, exp1.Equal(found))
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
			AggType:   "sum",
			Durations: []uint64{3 * 3600, 6 * 3600, 120},
		},
		Id: 1,
	}
	key := value.Int(4)
	keystr := key.String()
	assert.Equal(t, int64(t0), tier.Clock.Now())
	assert.NoError(t, aggregate2.Store(ctx, tier, agg))
	// initially count is zero
	valueSendReceive(t, holder, agg, key, value.Int(0), value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}))

	// now create an increment
	h := counter.NewSum(agg.Options.Durations)
	t1 := t0 + 3600
	buckets := h.BucketizeMoment(keystr, t1, value.Int(1))
	err = counter.Update(context.Background(), tier, agg.Id, buckets, h)
	assert.NoError(t, err)
	clock.Set(int64(t1 + 60))
	valueSendReceive(t, holder, agg, key, value.Int(1), value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}))

	// create another increment at a later timestamp
	t2 := t1 + 3600
	buckets = h.BucketizeMoment(keystr, t2, value.Int(1))
	err = counter.Update(context.Background(), tier, agg.Id, buckets, h)
	assert.NoError(t, err)
	clock.Set(int64(t2 + 60))
	valueSendReceive(t, holder, agg, key, value.Int(2), value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}))
	valueSendReceive(t, holder, agg, key, value.Int(1), value.NewDict(map[string]value.Value{"duration": value.Int(120)}))
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
			AggType:   "sum",
			Durations: []uint64{3 * 3600, 6 * 3600, 1800},
		},
	}
	agg2 := aggregate.Aggregate{
		Name:      "maxelem",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:   "max",
			Durations: []uint64{3 * 3600, 6 * 3600},
		},
	}
	assert.NoError(t, aggregate2.Store(ctx, tier, agg1))
	assert.NoError(t, aggregate2.Store(ctx, tier, agg2))

	// agg1 is assigned Id = 1 & agg2.Id = 2
	agg1.Id = 1
	agg2.Id = 2

	// now create changes
	t1 := t0 + 3600
	key := value.Nil
	keystr := key.String()

	h1 := counter.NewSum(agg1.Options.Durations)
	buckets := h1.BucketizeMoment(keystr, t1, value.Int(1))
	err = counter.Update(context.Background(), tier, agg1.Id, buckets, h1)
	assert.NoError(t, err)
	buckets = h1.BucketizeMoment(keystr, t1, value.Int(3))
	err = counter.Update(context.Background(), tier, agg1.Id, buckets, h1)
	assert.NoError(t, err)
	req1 := aggregate.GetAggValueRequest{
		AggName: agg1.Name, Key: key, Kwargs: value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
	}

	h2 := counter.NewMax(agg2.Options.Durations)
	buckets = h2.BucketizeMoment(keystr, t1, value.NewList(value.Int(2), value.Bool(false)))
	err = counter.Update(context.Background(), tier, agg2.Id, buckets, h2)
	assert.NoError(t, err)
	buckets = h2.BucketizeMoment(keystr, t1, value.NewList(value.Int(7), value.Bool(false)))
	err = counter.Update(context.Background(), tier, agg2.Id, buckets, h2)
	assert.NoError(t, err)
	req2 := aggregate.GetAggValueRequest{
		AggName: agg2.Name, Key: key, Kwargs: value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
	}

	clock.Set(int64(t1 + 60))
	batchValueSendReceive(t, holder,
		[]aggregate.GetAggValueRequest{req1, req2}, []value.Value{value.Int(4), value.Int(7)})

	// create some more changes at a later timestamp
	t2 := t1 + 3600
	buckets = h1.BucketizeMoment(keystr, t2, value.Int(9))
	err = counter.Update(context.Background(), tier, agg1.Id, buckets, h1)
	assert.NoError(t, err)
	req3 := aggregate.GetAggValueRequest{
		AggName: agg1.Name, Key: key, Kwargs: value.NewDict(map[string]value.Value{"duration": value.Int(1800)}),
	}

	clock.Set(int64(t2 + 60))
	batchValueSendReceive(t, holder,
		[]aggregate.GetAggValueRequest{req1, req2, req3}, []value.Value{value.Int(13), value.Int(7), value.Int(9)})
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
			AggType:   "sum",
			Durations: []uint64{3600 * 24, 3600 * 12},
		},
		Timestamp: 123,
	}
	err = c.StoreAggregate(agg)
	assert.NoError(t, err)
	found, err := c.RetrieveAggregate("mycounter")
	assert.NoError(t, err)
	// Id is set when an entry is created in the DB
	expected := agg
	expected.Id = 1
	expected.Active = true
	assert.Equal(t, expected, found)
	// trying to rewrite the same agg name throws an error even if query/options are different
	agg2 := aggregate.Aggregate{
		Name:  "mycounter",
		Query: ast.MakeDouble(3.4),
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint64{3600 * 24 * 2, 3600 * 24},
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
	expected = agg2
	// second row there
	expected.Id = 2
	expected.Active = true
	assert.Equal(t, expected, found)

	// can retrieve after deactivating, but "active" is not set.
	err = c.DeactivateAggregate(agg.Name)
	assert.NoError(t, err)
	found, err = c.RetrieveAggregate(agg.Name)
	assert.NoError(t, err)
	assert.False(t, found.Active)

	// but can deactivate again without error
	err = c.DeactivateAggregate(agg.Name)
	assert.NoError(t, err)

	// but cannot deactivate aggregate that does not exist
	err = c.DeactivateAggregate("nonexistent aggregate")
	assert.Error(t, err)
}

func checkSet(t *testing.T, c *client.Client, otype string, oid string,
	updateTime uint64, key string, val value.Value) profilelib.ProfileItem {
	profile := profilelib.ProfileItem{OType: ftypes.OType(otype), Oid: oid, Key: key, UpdateTime: updateTime, Value: val}
	err := c.SetProfile(&profile)
	assert.NoError(t, err)
	return profile
}

func valueSendReceive(t *testing.T, controller server, agg aggregate.Aggregate, key, expected value.Value, kwargs *value.Dict) {
	aggregate2.InvalidateCache() // invalidate cache, as it is not being tested here
	gavr := aggregate.GetAggValueRequest{AggName: agg.Name, Key: key, Kwargs: kwargs}
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
	aggregate2.InvalidateCache() // invalidate cache, as it is not being tested here
	ser, err := json.Marshal(reqs)
	assert.NoError(t, err)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/batch_aggregate_value", strings.NewReader(string(ser)))
	controller.BatchAggregateValue(w, r)
	// convert vals to json and compare
	found, err := ioutil.ReadAll(w.Body)
	assert.NoError(t, err)
	expected, err := json.Marshal(expectedVals)
	assert.Equal(t, string(expected), string(found))
}

func batchReadProfilesFromConsumer(t *testing.T, ctx context.Context, consumer kafka.FConsumer, upto int) ([]profilelib.ProfileItem, error) {
	actualp, err := consumer.ReadBatch(ctx, upto, time.Second*10)
	assert.NoError(t, err)
	actual := make([]profilelib.ProfileItem, len(actualp))
	for i := range actualp {
		var p profilelib.ProtoProfileItem
		if err = proto.Unmarshal(actualp[i], &p); err != nil {
			return nil, err
		}
		if actual[i], err = profilelib.FromProtoProfileItem(&p); err != nil {
			return nil, err
		}
	}
	return actual, nil
}
