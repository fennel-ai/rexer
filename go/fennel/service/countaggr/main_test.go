package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"fennel/controller/action"
	"fennel/controller/aggregate"
	"fennel/engine/ast"
	"fennel/kafka"
	actionlib "fennel/lib/action"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"
	"fennel/tier"

	"github.com/stretchr/testify/assert"
)

type scenario struct {
	agg      libaggregate.Aggregate
	initial  value.Value
	key      value.Value
	kwargs   []value.Dict
	expected []value.Value
	consumer kafka.FConsumer
}

func TestEndToEnd(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	uid := ftypes.OidType(1312)
	scenarios := []*scenario{
		{
			libaggregate.Aggregate{
				Name: "agg_1", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "sum", Duration: 6 * 3600},
			},
			value.Int(0),
			value.Int(uid),
			[]value.Dict{{}, {"duration": value.Int(3600)}},
			[]value.Value{value.Int(3), value.Int(2)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_2", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "timeseries_sum", Window: ftypes.Window_HOUR, Limit: 4},
			},
			value.List{value.Int(0), value.Int(0), value.Int(0), value.Int(0)},
			value.Int(uid),
			[]value.Dict{{}},
			[]value.Value{value.List{value.Int(0), value.Int(0), value.Int(1), value.Int(2)}},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_3", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "list", Duration: 6 * 3600},
			},
			value.List{},
			value.Int(uid),
			[]value.Dict{{}, {"duration": value.Int(3600)}},
			[]value.Value{value.List{value.Int(1), value.Int(2)}, value.List{value.Int(2)}},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_4", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "min", Duration: 6 * 3600},
			},
			value.Int(0),
			value.Int(uid),
			[]value.Dict{{}, {"duration": value.Int(3600)}},
			[]value.Value{value.Int(1), value.Int(2)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_5", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "max", Duration: 6 * 3600},
			},
			value.Int(0),
			value.Int(uid),
			[]value.Dict{{}, {"duration": value.Int(3600)}},
			[]value.Value{value.Int(2), value.Int(2)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_6", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "stddev", Duration: 6 * 3600},
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{{}, {"duration": value.Int(3600)}},
			[]value.Value{value.Double(0.5), value.Double(0)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_7", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "average", Duration: 6 * 3600},
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{{}, {"duration": value.Int(3600)}},
			[]value.Value{value.Double(1.5), value.Double(2)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_8", Query: getQueryRate(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "rate", Duration: 6 * 3600, Normalize: true},
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{{}, {"duration": value.Int(3600)}},
			[]value.Value{value.Double(0.15003570882017145), value.Double(0.09452865480086611)},
			nil,
		},
	}
	clock := &test.FakeClock{}
	tier.Clock = clock
	t0 := ftypes.Timestamp(time.Hour * 24 * 15)
	clock.Set(int64(t0))

	for _, scenario := range scenarios {
		// first store all aggregates
		assert.NoError(t, aggregate.Store(ctx, tier, scenario.agg))
		// and verify initial value is right
		for i := range scenario.kwargs {
			verify(t, tier, scenario.agg, scenario.key, scenario.kwargs[i], scenario.initial)
		}

		// next create kafka consumers for each
		scenario.consumer, err = tier.NewKafkaConsumer(actionlib.ACTIONLOG_KAFKA_TOPIC, string(scenario.agg.Name), kafka.DefaultOffsetPolicy)
		assert.NoError(t, err)
		defer scenario.consumer.Close()
	}

	// now fire a few actions
	actions1 := logAction(t, tier, uid, t0+ftypes.Timestamp(1), value.Dict{"value": value.Int(1)})
	actions2 := logAction(t, tier, uid, t0+ftypes.Timestamp(4000), value.Dict{"value": value.Int(2)})
	actions := append(actions1, actions2...)

	t1 := t0 + 7200
	clock.Set(int64(t1))
	// counts don't change until we run process, after which, they do
	for _, scenario := range scenarios {
		for i := range scenario.kwargs {
			verify(t, tier, scenario.agg, scenario.key, scenario.kwargs[i], scenario.initial)
		}
	}
	processInParallel(tier, scenarios)
	// now the counts should have updated
	for _, scenario := range scenarios {
		for i := range scenario.kwargs {
			verify(t, tier, scenario.agg, scenario.key, scenario.kwargs[i], scenario.expected[i])
		}
	}

	// unrelatedly, actions get inserted in DB with one iteration of insertInDB
	found, err := action.Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	assert.Empty(t, found)
	consumer, err := tier.NewKafkaConsumer(actionlib.ACTIONLOG_KAFKA_TOPIC, "insert_in_db", kafka.DefaultOffsetPolicy)
	assert.NoError(t, err)
	defer consumer.Close()
	assert.NoError(t, action.TransferToDB(ctx, tier, consumer))
	found, err = action.Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	for i, a := range actions {
		assert.True(t, a.Equals(found[i], true))
	}
}

func processInParallel(tier tier.Tier, scenarios []*scenario) {
	wg := sync.WaitGroup{}
	wg.Add(len(scenarios))
	for _, sc := range scenarios {
		go func(s *scenario) {
			defer wg.Done()
			aggregate.Update(context.Background(), tier, s.consumer, s.agg)
		}(sc)
	}
	wg.Wait()
}

func verify(t *testing.T, tier tier.Tier, agg libaggregate.Aggregate, k value.Value, kwargs value.Dict, expected interface{}) {
	found, err := aggregate.Value(context.Background(), tier, agg.Name, k, kwargs)
	assert.NoError(t, err)
	// for floats, it's best to not do direct equality comparison but verify their differnce is small
	if _, ok := expected.(value.Double); ok {
		asfloat, ok := found.(value.Double)
		assert.True(t, ok)
		assert.True(t, float64(expected.(value.Double)-asfloat) < 1e-6)
	} else {
		assert.Equal(t, expected, found)
	}
}

func logAction(t *testing.T, tier tier.Tier, uid ftypes.OidType, ts ftypes.Timestamp, metadata value.Value) []actionlib.Action {
	a1 := actionlib.Action{
		ActorID:    uid,
		ActorType:  "user",
		TargetID:   10,
		TargetType: "video",
		ActionType: "like",
		Metadata:   metadata,
		Timestamp:  ts,
		RequestID:  12,
	}
	a2 := a1
	a2.ActionType = "share"
	err := action.Insert(context.Background(), tier, a1)
	assert.NoError(t, err)
	err = action.Insert(context.Background(), tier, a2)
	assert.NoError(t, err)
	return []actionlib.Action{a1, a2}
}

func getQueryRate() ast.Ast {
	return ast.OpCall{
		Operand: ast.OpCall{
			Operand: ast.OpCall{
				Operand:   ast.Lookup{On: ast.Var{Name: "args"}, Property: "actions"},
				Namespace: "std",
				Name:      "filter",
				Kwargs: ast.Dict{Values: map[string]ast.Ast{
					"where": ast.Binary{
						Left:  ast.Lookup{On: ast.At{}, Property: "action_type"},
						Op:    "==",
						Right: ast.MakeString("like"),
					},
				}},
			},
			Namespace: "std",
			Name:      "addField",
			Kwargs: ast.Dict{Values: map[string]ast.Ast{
				"name": ast.MakeString("groupkey"),
				"value": ast.Lookup{
					On:       ast.At{},
					Property: "actor_id",
				}},
			},
		},
		Namespace: "std",
		Name:      "addField",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"name":  ast.MakeString("value"),
			"value": ast.List{Values: []ast.Ast{ast.MakeInt(1), ast.MakeInt(2)}},
		}},
	}
}

func getQuery() ast.Ast {
	return ast.OpCall{
		Operand: ast.OpCall{
			Operand: ast.OpCall{
				Operand:   ast.Lookup{On: ast.Var{Name: "args"}, Property: "actions"},
				Namespace: "std",
				Name:      "filter",
				Kwargs: ast.Dict{Values: map[string]ast.Ast{
					"where": ast.Binary{
						Left:  ast.Lookup{On: ast.At{}, Property: "action_type"},
						Op:    "==",
						Right: ast.MakeString("like"),
					},
				}},
			},
			Namespace: "std",
			Name:      "addField",
			Kwargs: ast.Dict{Values: map[string]ast.Ast{
				"name": ast.MakeString("groupkey"),
				"value": ast.Lookup{
					On:       ast.At{},
					Property: "actor_id",
				}},
			},
		},
		Namespace: "std",
		Name:      "addField",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"name": ast.MakeString("value"),
			"value": ast.Lookup{
				On: ast.Lookup{
					On:       ast.At{},
					Property: "metadata",
				},
				Property: "value",
			},
		}},
	}
}
