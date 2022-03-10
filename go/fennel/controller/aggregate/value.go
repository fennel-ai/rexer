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

func Value(ctx context.Context, tier tier.Tier, name ftypes.AggName, key value.Value) (value.Value, error) {
	agg, err := Retrieve(ctx, tier, name)
	if err != nil {
		return value.Nil, err
	}
	histogram, err := toHistogram(agg)
	if err != nil {
		return nil, err
	}
	return counter.Value(ctx, tier, key, histogram)
}

func BatchValue(ctx context.Context, tier tier.Tier, batch []aggregate.GetAggValueRequest) ([]value.Value, error) {
	ret := make([]value.Value, len(batch))
	for i, req := range batch {
		v, err := Value(ctx, tier, req.AggName, req.Key)
		if err != nil {
			return nil, err
		}
		ret[i] = v
	}
	return ret, nil
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
	if err = counter.Update(ctx, tier, table, histogram); err != nil {
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
		return value.List{}, err
	}
	result, err := interpreter.Eval(query, value.Dict{"actions": table})
	if err != nil {
		return value.List{}, err
	}
	table, ok := result.(value.List)
	if !ok {
		return value.List{}, fmt.Errorf("query did not transform actions into a list")
	}
	return table, nil
}

func toHistogram(agg aggregate.Aggregate) (modelCounter.Histogram, error) {
	switch agg.Options.AggType {
	case "sum":
		return modelCounter.NewSum(agg.Name, agg.Options.Duration), nil
	case "timeseries_sum":
		return modelCounter.NewTimeseriesSum(agg.Name, agg.Options.Window, agg.Options.Limit), nil
	case "average":
		return modelCounter.NewAverage(agg.Name, agg.Options.Duration), nil
	case "list":
		return modelCounter.NewList(agg.Name, agg.Options.Duration), nil
	case "min":
		return modelCounter.NewMin(agg.Name, agg.Options.Duration), nil
	case "max":
		return modelCounter.NewMax(agg.Name, agg.Options.Duration), nil
	case "stddev":
		return modelCounter.NewStdDev(agg.Name, agg.Options.Duration), nil
	case "rate":
		return modelCounter.NewRate(agg.Name, agg.Options.Duration, agg.Options.Normalize), nil
	default:
		return nil, fmt.Errorf("invalid aggregate type: %v", agg.Options.AggType)
	}
}
