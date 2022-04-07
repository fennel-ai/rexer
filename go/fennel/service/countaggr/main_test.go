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
				Options: libaggregate.Options{AggType: "sum", Durations: []uint64{3 * 3600, 6 * 3600, 3600}},
			},
			value.Int(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Int(3), value.Int(2)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_2", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "timeseries_sum", Window: ftypes.Window_HOUR, Limit: 4},
			},
			value.NewList(value.Int(0), value.Int(0), value.Int(0), value.Int(0)),
			value.Int(uid),
			[]value.Dict{value.NewDict(map[string]value.Value{})},
			[]value.Value{value.NewList(value.Int(0), value.Int(0), value.Int(1), value.Int(2))},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_3", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "list", Durations: []uint64{3 * 3600, 6 * 3600, 3600}},
			},
			value.NewList(),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)}),
			},
			[]value.Value{value.NewList(value.Int(1), value.Int(2)), value.NewList(value.Int(2))},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_4", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "min", Durations: []uint64{3 * 3600, 6 * 3600, 3600}},
			},
			value.Int(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Int(1), value.Int(2)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_5", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "max", Durations: []uint64{3 * 3600, 6 * 3600, 3600}},
			},
			value.Int(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Int(2), value.Int(2)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_6", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "stddev", Durations: []uint64{3 * 3600, 6 * 3600, 3600}},
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Double(0.5), value.Double(0)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_7", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "average", Durations: []uint64{3 * 3600, 6 * 3600, 3600}},
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Double(1.5), value.Double(2)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_8", Query: getQueryRate(), Timestamp: 123,
				Options: libaggregate.Options{
					AggType:   "rate",
					Durations: []uint64{3 * 3600, 6 * 3600, 3600},
					Normalize: true,
				},
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Double(0.15003570882017145), value.Double(0.09452865480086611)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_9", Query: getQueryTopK(), Timestamp: 123,
				Options: libaggregate.Options{AggType: "topk", Durations: []uint64{3 * 3600, 6 * 3600, 3600}},
			},
			value.NewList(),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{
				value.NewList(
					value.NewList(value.String("like"), value.Double(3)),
					value.NewList(value.String("share"), value.Double(1)),
				),
				value.NewList(
					value.NewList(value.String("like"), value.Double(2)),
					value.NewList(value.String("share"), value.Double(0.5)),
				)},
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
	actions1 := logAction(t, tier, uid, t0+ftypes.Timestamp(1), value.NewDict(map[string]value.Value{"value": value.Int(1)}))
	actions2 := logAction(t, tier, uid, t0+ftypes.Timestamp(4000), value.NewDict(map[string]value.Value{"value": value.Int(2)}))
	actions := append(actions1, actions2...)

	t1 := t0 + 7200
	clock.Set(int64(t1))
	// counts don't change until we run process, after which, they do
	for _, scenario := range scenarios {
		for i := range scenario.kwargs {
			verify(t, tier, scenario.agg, scenario.key, scenario.kwargs[i], scenario.initial)
		}
	}
	processInParallel(t, tier, scenarios)
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

func processInParallel(t *testing.T, tier tier.Tier, scenarios []*scenario) {
	wg := sync.WaitGroup{}
	wg.Add(len(scenarios))
	for _, sc := range scenarios {
		go func(s *scenario) {
			defer wg.Done()
			err := aggregate.Update(context.Background(), tier, s.consumer, s.agg)
			assert.NoError(t, err)
		}(sc)
	}
	wg.Wait()
}

func verify(t *testing.T, tier tier.Tier, agg libaggregate.Aggregate, k value.Value, kwargs value.Dict, expected interface{}) {
	aggregate.InvalidateCache() // invalidate cache, as it is not being tested here
	found, err := aggregate.Value(context.Background(), tier, agg.Name, k, kwargs)
	assert.NoError(t, err)
	// for floats, it's best to not do direct equality comparison but verify their differnce is small
	if _, ok := expected.(value.Double); ok {
		asfloat, ok := found.(value.Double)
		assert.True(t, ok)
		assert.True(t, float64(expected.(value.Double)-asfloat) < 1e-6)
	} else {
		assert.Equal(t, expected, found, agg)
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
	a2.Metadata = value.NewDict(map[string]value.Value{"value": value.Double(0.5)})
	err := action.Insert(context.Background(), tier, a1)
	assert.NoError(t, err)
	err = action.Insert(context.Background(), tier, a2)
	assert.NoError(t, err)
	return []actionlib.Action{a1, a2}
}

func getQueryRate() ast.Ast {
	return ast.OpCall{
		Operands: []ast.Ast{ast.OpCall{
			Operands: []ast.Ast{ast.OpCall{
				Operands:  []ast.Ast{ast.Var{Name: "actions"}},
				Vars:      []string{"hi"},
				Namespace: "std",
				Name:      "filter",
				Kwargs: ast.Dict{Values: map[string]ast.Ast{
					"where": ast.Binary{
						Left:  ast.Lookup{On: ast.Var{Name: "hi"}, Property: "action_type"},
						Op:    "==",
						Right: ast.MakeString("like"),
					},
				}},
			}},
			Vars:      []string{"okay"},
			Namespace: "std",
			Name:      "set",
			Kwargs: ast.Dict{Values: map[string]ast.Ast{
				"field": ast.MakeString("groupkey"),
				"value": ast.Lookup{
					On:       ast.Var{Name: "okay"},
					Property: "actor_id",
				}},
			},
		}},
		Namespace: "std",
		Name:      "set",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"field": ast.MakeString("value"),
			"value": ast.List{Values: []ast.Ast{ast.MakeInt(1), ast.MakeInt(2)}},
		}},
	}
}

func getQuery() ast.Ast {
	return ast.OpCall{
		Namespace: "std",
		Name:      "set",
		Operands: []ast.Ast{ast.OpCall{
			Namespace: "std",
			Name:      "set",
			Operands: []ast.Ast{ast.OpCall{
				Namespace: "std",
				Name:      "filter",
				Operands:  []ast.Ast{ast.Var{Name: "actions"}},
				Vars:      []string{"v"},
				Kwargs: ast.Dict{Values: map[string]ast.Ast{
					"where": ast.Binary{
						Left:  ast.Lookup{On: ast.Var{Name: "v"}, Property: "action_type"},
						Op:    "==",
						Right: ast.MakeString("like"),
					},
				}},
			}},
			Vars: []string{"at"},
			Kwargs: ast.Dict{Values: map[string]ast.Ast{
				"field": ast.MakeString("groupkey"),
				"value": ast.Lookup{
					On:       ast.Var{Name: "at"},
					Property: "actor_id",
				}},
			},
		}},
		Vars: []string{"it"},
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"field": ast.MakeString("value"),
			"value": ast.Lookup{
				On: ast.Lookup{
					On:       ast.Var{Name: "it"},
					Property: "metadata",
				},
				Property: "value",
			},
		}},
	}
}

func getQueryTopK() ast.Ast {
	return ast.OpCall{
		Operands: []ast.Ast{ast.OpCall{
			Operands:  []ast.Ast{ast.Var{Name: "actions"}},
			Vars:      []string{"at"},
			Namespace: "std",
			Name:      "set",
			Kwargs: ast.Dict{Values: map[string]ast.Ast{
				"field": ast.MakeString("groupkey"),
				"value": ast.Lookup{
					On:       ast.Var{Name: "at"},
					Property: "actor_id",
				}},
			},
		}},
		Vars:      []string{"at"},
		Namespace: "std",
		Name:      "set",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"field": ast.MakeString("value"),
			"value": ast.Dict{Values: map[string]ast.Ast{
				"key": ast.Lookup{
					On:       ast.Var{Name: "at"},
					Property: "action_type",
				},
				"score": ast.Lookup{
					On: ast.Lookup{
						On:       ast.Var{Name: "at"},
						Property: "metadata",
					},
					Property: "value",
				},
			}},
		}},
	}
}
