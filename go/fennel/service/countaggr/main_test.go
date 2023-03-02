package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	clock2 "github.com/raulk/clock"

	profile2 "fennel/controller/profile"
	profilelib "fennel/lib/profile"
	"fennel/resource"
	"fennel/test/nitrous"

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
}

func TestEndToEndActionAggregates(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	clock := tier.Clock.(*clock2.Mock)
	clock.Set(time.Now())

	ctx := context.Background()
	uid := 1312
	scenarios := []*scenario{
		{
			libaggregate.Aggregate{
				Name: "agg_1", Query: getQuery(), Timestamp: 123,
				Source:  libaggregate.SOURCE_ACTION,
				Options: libaggregate.Options{AggType: "sum", Durations: []uint32{3 * 3600, 6 * 3600, 3600}},
				Id:      1,
			},
			value.Int(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Int(3), value.Int(2)},
		},
		{
			libaggregate.Aggregate{
				Name: "agg_3", Query: getQuery(), Timestamp: 123,
				Source:  libaggregate.SOURCE_ACTION,
				Options: libaggregate.Options{AggType: "list", Durations: []uint32{3 * 3600, 6 * 3600, 3600}},
				Id:      2,
			},
			value.NewList(),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)}),
			},
			[]value.Value{value.NewList(value.Int(1), value.Int(2)), value.NewList(value.Int(2))},
		},
		{
			libaggregate.Aggregate{
				Name: "agg_4", Query: getQuery(), Timestamp: 123,
				Source:  libaggregate.SOURCE_ACTION,
				Options: libaggregate.Options{AggType: "min", Durations: []uint32{3 * 3600, 6 * 3600, 3600}},
				Id:      3,
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Int(1), value.Int(2)},
		},
		{
			libaggregate.Aggregate{
				Name: "agg_5", Query: getQuery(), Timestamp: 123,
				Source:  libaggregate.SOURCE_ACTION,
				Options: libaggregate.Options{AggType: "max", Durations: []uint32{3 * 3600, 6 * 3600, 3600}},
				Id:      4,
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Int(2), value.Int(2)},
		},
		{
			libaggregate.Aggregate{
				Name: "agg_6", Query: getQuery(), Timestamp: 123,
				Source:  libaggregate.SOURCE_ACTION,
				Options: libaggregate.Options{AggType: "stddev", Durations: []uint32{3 * 3600, 6 * 3600, 3600}},
				Id:      5,
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Double(0.5), value.Double(0)},
		},
		{
			libaggregate.Aggregate{
				Name: "agg_7", Query: getQuery(), Timestamp: 123,
				Source:  libaggregate.SOURCE_ACTION,
				Options: libaggregate.Options{AggType: "average", Durations: []uint32{3 * 3600, 6 * 3600, 3600}},
				Id:      6,
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Double(1.5), value.Double(2)},
		},
		{
			libaggregate.Aggregate{
				Name: "agg_8", Query: getQueryRate(), Timestamp: 123,
				Source: libaggregate.SOURCE_ACTION,
				Options: libaggregate.Options{
					AggType:   "rate",
					Durations: []uint32{3 * 3600, 6 * 3600, 3600},
					Normalize: true,
				},
				Id: 7,
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Double(0.15003570882017145), value.Double(0.09452865480086611)},
		},
		{
			libaggregate.Aggregate{
				Name: "agg_9", Query: getQueryTopK(), Timestamp: 123,
				Source:  libaggregate.SOURCE_ACTION,
				Options: libaggregate.Options{AggType: "topk", Durations: []uint32{3 * 3600, 6 * 3600, 3600}, Limit: 1},
				Id:      8,
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
		},
		// TODO(mohit): Support for TIMESERIES_SUM is not well handled in Nitrous. Enable this once there is a good
		// support for it
		// {
		// 	libaggregate.Aggregate{
		// 		Name: "agg_2", Query: getQuery(), Timestamp: 123,
		// 		Source:  libaggregate.SOURCE_ACTION,
		// 		Mode:    "rql",
		// 		Options: libaggregate.Options{AggType: libaggregate.TIMESERIES_SUM, Window: ftypes.Window_HOUR, Limit: 4},
		// 		Id:      9,
		// 	},
		// 	value.NewList(value.Int(0), value.Int(0), value.Int(0), value.Int(0)),
		// 	value.Int(uid),
		// 	[]value.Dict{value.NewDict(nil)},
		// 	[]value.Value{value.NewList(value.Int(0), value.Int(0), value.Int(1), value.Int(2))},
		// },
	}
	t0 := clock.Now()
	t1 := t0.Add(3600 * 24 * 15 * time.Second)
	clock.Set(t1)

	for _, scenario := range scenarios {
		// first store all aggregates
		assert.NoError(t, aggregate.Store(ctx, tier, scenario.agg))
		nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)
		// and verify initial value is right
		for i := range scenario.kwargs {
			verify(t, tier, scenario.agg, scenario.key, scenario.kwargs[i], scenario.initial)
		}
	}

	// now fire a few actions
	actions1 := logAction(t, tier, ftypes.OidType(strconv.Itoa(uid)), ftypes.Timestamp(t1.Unix())+ftypes.Timestamp(1), value.NewDict(map[string]value.Value{"value": value.Int(1)}))
	actions2 := logAction(t, tier, ftypes.OidType(strconv.Itoa(uid)), ftypes.Timestamp(t1.Unix())+ftypes.Timestamp(4000), value.NewDict(map[string]value.Value{"value": value.Int(2)}))
	actions := append(actions1, actions2...)

	t2 := t1.Add(7200 * time.Second)
	clock.Set(t2)
	// counts don't change until we run process, after which, they do
	for _, scenario := range scenarios {
		for i := range scenario.kwargs {
			verify(t, tier, scenario.agg, scenario.key, scenario.kwargs[i], scenario.initial)
		}
	}
	processInParallel(t, tier, scenarios, actions)
	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)
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
	consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Scope:        resource.NewTierScope(tier.ID),
		Topic:        actionlib.ACTIONLOG_KAFKA_TOPIC,
		GroupID:      "insert_in_db",
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	assert.NoError(t, err)
	defer consumer.Close()
	assert.NoError(t, action.TransferToDB(ctx, tier, consumer))
	found, err = action.Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	fmt.Printf("found: %v actions: %v\n", found, actions)
	sort.Slice(actions, func(i, j int) bool {
		return actions[i].Timestamp > actions[j].Timestamp
	})
	for i, a := range actions {
		assert.True(t, a.Equals(found[i], true))
	}
}

func TestEndToEndProfileAggregates(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	ctx := context.Background()
	uid := 1312
	scenarios := []*scenario{
		{
			libaggregate.Aggregate{
				Name: "agg_prof_1", Query: getProfileQuery(), Timestamp: 123,
				Source:  libaggregate.SOURCE_PROFILE,
				Options: libaggregate.Options{AggType: "sum", Durations: []uint32{3 * 3600, 6 * 3600, 3600}},
				Id:      1,
			},
			value.Int(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Int(3), value.Int(2)},
		},
		{
			libaggregate.Aggregate{
				Name: "agg_prof_2", Query: getProfileQuery(), Timestamp: 123,
				Source:  libaggregate.SOURCE_PROFILE,
				Options: libaggregate.Options{AggType: "min", Durations: []uint32{3 * 3600, 6 * 3600, 3600}},
				Id:      2,
			},
			value.Double(0),
			value.Int(uid),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"duration": value.Int(6 * 3600)}),
				value.NewDict(map[string]value.Value{"duration": value.Int(3600)})},
			[]value.Value{value.Int(1), value.Int(2)},
		},
	}
	clock := tier.Clock.(*clock2.Mock)
	t0 := clock.Now()
	t1 := t0.Add(3600 * 24 * 15 * time.Second)
	clock.Set(t1)

	for _, scenario := range scenarios {
		// first store all aggregates
		assert.NoError(t, aggregate.Store(ctx, tier, scenario.agg))
		nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)
		// and verify initial value is right
		for i := range scenario.kwargs {
			verify(t, tier, scenario.agg, scenario.key, scenario.kwargs[i], scenario.initial)
		}
	}

	// now fire a few profiles
	p1 := logProfile(t, tier, ftypes.OidType(strconv.Itoa(uid)), uint64(ftypes.Timestamp(t1.Unix())+ftypes.Timestamp(1)), value.NewDict(map[string]value.Value{"value": value.Int(1)}))
	p2 := logProfile(t, tier, ftypes.OidType(strconv.Itoa(uid)), uint64(ftypes.Timestamp(t1.Unix())+ftypes.Timestamp(4000)), value.NewDict(map[string]value.Value{"value": value.Int(2)}))
	profiles := append(p1, p2...)
	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	t2 := t1.Add(7200 * time.Second)
	clock.Set(t2)
	// counts don't change until we run process, after which, they do
	for _, scenario := range scenarios {
		for i := range scenario.kwargs {
			verify(t, tier, scenario.agg, scenario.key, scenario.kwargs[i], scenario.initial)
		}
	}
	processInParallel(t, tier, scenarios, profiles)
	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	// now the counts should have updated
	for _, scenario := range scenarios {
		for i := range scenario.kwargs {
			verify(t, tier, scenario.agg, scenario.key, scenario.kwargs[i], scenario.expected[i])
		}
	}

	profRequest := []profilelib.ProfileItemKey{profiles[0].GetProfileKey(), profiles[1].GetProfileKey()}
	found, err := profile2.GetBatch(ctx, tier, profRequest)
	assert.NoError(t, err)
	assert.Empty(t, found[0].Value)
	assert.Empty(t, found[1].Value)

	consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Scope:        resource.NewTierScope(tier.ID),
		Topic:        profilelib.PROFILELOG_KAFKA_TOPIC,
		GroupID:      "insert_in_db",
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	assert.NoError(t, err)
	defer consumer.Close()
	assert.NoError(t, profile2.TransferToDB(ctx, tier, consumer))

	found, err = profile2.GetBatch(ctx, tier, profRequest)
	assert.NoError(t, err)
	expectedProfile := profiles[1]
	expectedProfile.UpdateTime = 0
	assert.Equal(t, expectedProfile, found[0])
	assert.Equal(t, expectedProfile, found[1])

}
func processInParallel[I actionlib.Action | profilelib.ProfileItem](t *testing.T, tier tier.Tier, scenarios []*scenario, items []I) {
	wg := sync.WaitGroup{}
	wg.Add(len(scenarios))
	ctx := context.Background()
	for i := 0; i < len(scenarios); i++ {
		go func(scene *scenario) {
			defer wg.Done()
			err := aggregate.Update(ctx, tier, items, scene.agg)
			assert.NoError(t, err)
		}(scenarios[i])
	}
	wg.Wait()
}

func verify(t *testing.T, tier tier.Tier, agg libaggregate.Aggregate, k value.Value, kwargs value.Dict, expected interface{}) {
	aggregate.InvalidateCache() // invalidate cache, as it is not being tested here
	found, err := aggregate.Value(context.Background(), tier, agg.Name, k, kwargs)
	assert.NoError(t, err)
	// for floats, it's best to not do direct equality comparison but verify their difference is small
	if _, ok := expected.(value.Double); ok {
		asfloat, ok := found.(value.Double)
		assert.True(t, ok)
		assert.True(t, float64(expected.(value.Double)-asfloat) < 1e-6)
	} else {
		// list aggregate type behaves like a set, the ordering there is not deterministic
		if string(agg.Options.AggType) == "list" {
			e := expected.(value.Value).(value.List)
			f := found.(value.List)
			assert.ElementsMatch(t, e.Values(), f.Values(), agg)
		} else {
			assert.Equal(t, expected, found, agg)
		}
	}
}

func logAction(t *testing.T, tier tier.Tier, uid ftypes.OidType, ts ftypes.Timestamp, metadata value.Value) []actionlib.Action {
	a1 := actionlib.Action{
		ActorID:    uid,
		ActorType:  "user",
		TargetID:   "10",
		TargetType: "video",
		ActionType: "like",
		Metadata:   metadata,
		Timestamp:  ts,
		RequestID:  "12",
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

func logProfile(t *testing.T, tier tier.Tier, oid ftypes.OidType, ts uint64, val value.Value) []profilelib.ProfileItem {
	p := profilelib.ProfileItem{
		OType:      "user",
		Oid:        oid,
		Key:        "visited",
		Value:      val,
		UpdateTime: ts,
	}
	err := profile2.Set(context.Background(), tier, p)
	assert.NoError(t, err)
	return []profilelib.ProfileItem{p}
}

func getQueryRate() ast.Ast {
	return &ast.OpCall{
		Operands: []ast.Ast{&ast.OpCall{
			Operands: []ast.Ast{&ast.OpCall{
				Operands:  []ast.Ast{&ast.Var{Name: "actions"}},
				Vars:      []string{"hi"},
				Namespace: "std",
				Name:      "filter",
				Kwargs: ast.MakeDict(map[string]ast.Ast{
					"where": &ast.Binary{
						Left:  &ast.Lookup{On: &ast.Var{Name: "hi"}, Property: "action_type"},
						Op:    "==",
						Right: ast.MakeString("like"),
					},
				}),
			}},
			Vars:      []string{"okay"},
			Namespace: "std",
			Name:      "set",
			Kwargs: ast.MakeDict(map[string]ast.Ast{
				"field": ast.MakeString("groupkey"),
				"value": &ast.Lookup{
					On:       &ast.Var{Name: "okay"},
					Property: "actor_id",
				}},
			),
		}},
		Namespace: "std",
		Name:      "set",
		Kwargs: &ast.Dict{Values: map[string]ast.Ast{
			"field": ast.MakeString("value"),
			"value": ast.MakeList(ast.MakeInt(1), ast.MakeInt(2)),
		}},
	}
}

func getQuery() ast.Ast {
	return &ast.OpCall{
		Namespace: "std",
		Name:      "set",
		Operands: []ast.Ast{&ast.OpCall{
			Namespace: "std",
			Name:      "set",
			Operands: []ast.Ast{&ast.OpCall{
				Namespace: "std",
				Name:      "filter",
				Operands:  []ast.Ast{&ast.Var{Name: "actions"}},
				Vars:      []string{"v"},
				Kwargs: &ast.Dict{Values: map[string]ast.Ast{
					"where": &ast.Binary{
						Left:  &ast.Lookup{On: &ast.Var{Name: "v"}, Property: "action_type"},
						Op:    "==",
						Right: ast.MakeString("like"),
					},
				}},
			}},
			Vars: []string{"at"},
			Kwargs: &ast.Dict{Values: map[string]ast.Ast{
				"field": ast.MakeString("groupkey"),
				"value": &ast.Lookup{
					On:       &ast.Var{Name: "at"},
					Property: "actor_id",
				}},
			},
		}},
		Vars: []string{"it"},
		Kwargs: &ast.Dict{Values: map[string]ast.Ast{
			"field": ast.MakeString("value"),
			"value": &ast.Lookup{
				On: &ast.Lookup{
					On:       &ast.Var{Name: "it"},
					Property: "metadata",
				},
				Property: "value",
			},
		}},
	}
}

func getProfileQuery() ast.Ast {
	return &ast.OpCall{
		Namespace: "std",
		Name:      "set",
		Operands: []ast.Ast{&ast.OpCall{
			Namespace: "std",
			Name:      "set",
			Operands: []ast.Ast{&ast.OpCall{
				Namespace: "std",
				Name:      "filter",
				Operands:  []ast.Ast{&ast.Var{Name: "profiles"}},
				Vars:      []string{"v"},
				Kwargs: &ast.Dict{Values: map[string]ast.Ast{
					"where": &ast.Binary{
						Left:  &ast.Lookup{On: &ast.Var{Name: "v"}, Property: "otype"},
						Op:    "==",
						Right: ast.MakeString("user"),
					},
				}},
			}},
			Vars: []string{"at"},
			Kwargs: &ast.Dict{Values: map[string]ast.Ast{
				"field": ast.MakeString("groupkey"),
				"value": &ast.Lookup{
					On:       &ast.Var{Name: "at"},
					Property: "oid",
				}},
			},
		}},
		Vars: []string{"it"},
		Kwargs: &ast.Dict{Values: map[string]ast.Ast{
			"field": ast.MakeString("value"),
			"value": &ast.Lookup{
				On: &ast.Lookup{
					On:       &ast.Var{Name: "it"},
					Property: "value",
				},
				Property: "value",
			},
		}},
	}
}

func getQueryTopK() ast.Ast {
	return &ast.OpCall{
		Operands: []ast.Ast{&ast.OpCall{
			Operands:  []ast.Ast{&ast.Var{Name: "actions"}},
			Vars:      []string{"at"},
			Namespace: "std",
			Name:      "set",
			Kwargs: &ast.Dict{Values: map[string]ast.Ast{
				"field": ast.MakeString("groupkey"),
				"value": &ast.Lookup{
					On:       &ast.Var{Name: "at"},
					Property: "actor_id",
				}},
			},
		}},
		Vars:      []string{"at"},
		Namespace: "std",
		Name:      "set",
		Kwargs: &ast.Dict{Values: map[string]ast.Ast{
			"field": ast.MakeString("value"),
			"value": &ast.Dict{Values: map[string]ast.Ast{
				"key": &ast.Lookup{
					On:       &ast.Var{Name: "at"},
					Property: "action_type",
				},
				"score": &ast.Lookup{
					On: &ast.Lookup{
						On:       &ast.Var{Name: "at"},
						Property: "metadata",
					},
					Property: "value",
				},
			}},
		}},
	}
}
