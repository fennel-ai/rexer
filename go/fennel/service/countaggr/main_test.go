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
	expected value.Value
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
				Options: libaggregate.AggOptions{AggType: "count", Duration: 6 * 3600},
			},
			value.Int(0),
			value.Int(uid), value.Int(2),
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_2", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.AggOptions{AggType: "timeseries_count", Window: ftypes.Window_HOUR, Limit: 4},
			},
			value.List{value.Int(0), value.Int(0), value.Int(0), value.Int(0)},
			value.Int(uid), value.List{value.Int(0), value.Int(0), value.Int(2), value.Int(0)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_3", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.AggOptions{AggType: "list", Duration: 6 * 3600},
			},
			value.List{},
			value.Int(uid), value.List{value.Int(1), value.Int(2)},
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_4", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.AggOptions{AggType: "min", Duration: 6 * 3600},
			},
			value.Int(0),
			value.Int(uid), value.Int(1),
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_5", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.AggOptions{AggType: "max", Duration: 6 * 3600},
			},
			value.Int(0),
			value.Int(uid), value.Int(2),
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_6", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.AggOptions{AggType: "stddev", Duration: 6 * 3600},
			},
			value.Double(0),
			value.Int(uid), value.Double(0.5),
			nil,
		},
		{
			libaggregate.Aggregate{
				Name: "agg_7", Query: getQuery(), Timestamp: 123,
				Options: libaggregate.AggOptions{AggType: "average", Duration: 6 * 3600},
			},
			value.Double(0),
			value.Int(uid), value.Double(1.5),
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
		verify(t, tier, scenario.agg, scenario.key, scenario.initial)

		// next create kafka consumers for each
		scenario.consumer, err = tier.NewKafkaConsumer(actionlib.ACTIONLOG_KAFKA_TOPIC, string(scenario.agg.Name), kafka.DefaultOffsetPolicy)
		assert.NoError(t, err)
		defer scenario.consumer.Close()
	}

	// now fire a few actions
	logAction(t, tier, uid, t0+ftypes.Timestamp(1), value.Dict{"value": value.Int(1)})
	logAction(t, tier, uid, t0+ftypes.Timestamp(2), value.Dict{"value": value.Int(2)})

	t1 := t0 + 7200
	clock.Set(int64(t1))
	// counts don't change until we run process, after which, they do
	for _, scenario := range scenarios {
		verify(t, tier, scenario.agg, scenario.key, scenario.initial)
	}
	processInParallel(tier, scenarios)
	// now the counts should have updated
	for _, scenario := range scenarios {
		verify(t, tier, scenario.agg, scenario.key, scenario.expected)
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

func verify(t *testing.T, tier tier.Tier, agg libaggregate.Aggregate, k value.Value, expected interface{}) {
	found, err := aggregate.Value(context.Background(), tier, agg.Name, k)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}

func logAction(t *testing.T, tier tier.Tier, uid ftypes.OidType, ts ftypes.Timestamp, metadata value.Value) {
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
	_, err := action.Insert(context.Background(), tier, a1)
	assert.NoError(t, err)
	_, err = action.Insert(context.Background(), tier, a2)
	assert.NoError(t, err)
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
