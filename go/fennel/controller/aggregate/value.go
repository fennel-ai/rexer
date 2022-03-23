package aggregate

import (
	"context"
	"fmt"
	"time"

	"fennel/controller/action"
	"fennel/controller/counter"
	"fennel/engine/ast"
	"fennel/engine/interpreter"
	"fennel/engine/interpreter/bootarg"
	"fennel/kafka"
	libaction "fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	modelCounter "fennel/model/counter"
	"fennel/tier"
)

func Value(ctx context.Context, tier tier.Tier, name ftypes.AggName, key value.Value, kwargs value.Dict) (value.Value, error) {
	agg, err := Retrieve(ctx, tier, name)
	if err != nil {
		return value.Nil, err
	}
	histogram, err := toHistogram(agg)
	if err != nil {
		return nil, err
	}
	return counter.Value(ctx, tier, name, key, histogram, kwargs)
}

func BatchValue(ctx context.Context, tier tier.Tier, batch []aggregate.GetAggValueRequest) ([]value.Value, error) {
	n := len(batch)
	histograms := make([]modelCounter.Histogram, n)
	names := make([]ftypes.AggName, n)
	keys := make([]value.Value, n)
	kwargs := make([]value.Dict, n)
	for i, req := range batch {
		agg, err := Retrieve(ctx, tier, req.AggName)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve aggregate at index %d of batch: %v", i, err)
		}
		histograms[i], err = toHistogram(agg)
		if err != nil {
			return nil, fmt.Errorf("failed to make histogram from aggregate at index %d of batch: %v", i, err)
		}
		names[i] = req.AggName
		keys[i] = req.Key
		kwargs[i] = req.Kwargs
	}
	return counter.BatchValue(ctx, tier, names, keys, histograms, kwargs)
}

func Update(ctx context.Context, tier tier.Tier, consumer kafka.FConsumer, agg aggregate.Aggregate) error {
	actions, err := action.ReadBatch(ctx, consumer, 10000, time.Second*10)
	if err != nil {
		return err
	}
	tier.Logger.Info(fmt.Sprintf("found %d new actions for aggregate: %s", len(actions), agg.Name))
	if len(actions) == 0 {
		return nil
	}
	table, err := transformActions(tier, actions, agg.Query)

	if err != nil {
		return err
	}
	histogram, err := toHistogram(agg)
	if err != nil {
		return err
	}
	if err = counter.Update(ctx, tier, agg.Name, table, histogram); err != nil {
		return err
	}
	return consumer.Commit()
}

//============================
// Private helpers below
//============================

func transformActions(tier tier.Tier, actions []libaction.Action, query ast.Ast) (value.List, error) {
	bootargs := bootarg.Create(tier)
	interpreter := interpreter.NewInterpreter(bootargs)
	table, err := libaction.ToList(actions)
	if err != nil {
		return value.NewList(), err
	}
	result, err := interpreter.Eval(query, value.NewDict(map[string]value.Value{"actions": table}))
	if err != nil {
		return value.NewList(), err
	}
	table, ok := result.(value.List)
	if !ok {
		return value.NewList(), fmt.Errorf("query did not transform actions into a list")
	}
	return table, nil
}

func toHistogram(agg aggregate.Aggregate) (modelCounter.Histogram, error) {
	d := reduceMax(agg.Options.Durations)
	switch agg.Options.AggType {
	case "sum":
		return modelCounter.NewSum(agg.Name, d), nil
	case "timeseries_sum":
		return modelCounter.NewTimeseriesSum(agg.Name, agg.Options.Window, agg.Options.Limit), nil
	case "average":
		return modelCounter.NewAverage(agg.Name, d), nil
	case "list":
		return modelCounter.NewList(agg.Name, d), nil
	case "min":
		return modelCounter.NewMin(agg.Name, d), nil
	case "max":
		return modelCounter.NewMax(agg.Name, d), nil
	case "stddev":
		return modelCounter.NewStdDev(agg.Name, d), nil
	case "rate":
		return modelCounter.NewRate(agg.Name, d, agg.Options.Normalize), nil
	case "topk":
		return modelCounter.NewTopK(agg.Name, d), nil
	default:
		return nil, fmt.Errorf("invalid aggregate type: %v", agg.Options.AggType)
	}
}

func reduceMax(vals []uint64) uint64 {
	var max uint64 = 0
	for _, v := range vals {
		if v > max {
			max = v
		}
	}
	return max
}
