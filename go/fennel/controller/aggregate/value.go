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
	_ "fennel/opdefs/std"
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
	return counter.Value(ctx, tier, agg.Name, key, histogram)
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
	if err = counter.Update(ctx, tier, agg.Name, table, histogram); err != nil {
		return err
	}
	return consumer.Commit()
}

//============================
// Private helpers below
//============================

func transformActions(tier tier.Tier, actions []libaction.Action, query ast.Ast) (value.List, error) {
	interpreter, err := loadInterpreter(tier, actions)
	if err != nil {
		return value.List{}, err
	}
	result, err := query.AcceptValue(interpreter)
	if err != nil {
		return value.List{}, err
	}
	table, ok := result.(value.List)
	if !ok {
		return value.List{}, fmt.Errorf("query did not transform actions into a list")
	}
	return table, nil
}

func loadInterpreter(tier tier.Tier, actions []libaction.Action) (interpreter.Interpreter, error) {
	bootargs := bootarg.Create(tier)
	ret := interpreter.NewInterpreter(bootargs)
	table, err := libaction.ToList(actions)
	if err != nil {
		return ret, err
	}
	if err = ret.SetVar("args", value.Dict{"actions": table}); err != nil {
		return ret, err
	}
	return ret, nil
}

func toHistogram(agg aggregate.Aggregate) (modelCounter.Histogram, error) {
	switch agg.Options.AggType {
	case "sum":
		return modelCounter.RollingCounter{Duration: agg.Options.Duration}, nil
	case "timeseries_sum":
		return modelCounter.TimeseriesCounter{
			Window: agg.Options.Window, Limit: agg.Options.Limit,
		}, nil
	case "average":
		return modelCounter.RollingAverage{Duration: agg.Options.Duration}, nil
	case "list":
		return modelCounter.List{Duration: agg.Options.Duration}, nil
	case "min":
		return modelCounter.Min{Duration: agg.Options.Duration}, nil
	case "max":
		return modelCounter.Max{Duration: agg.Options.Duration}, nil
	case "stddev":
		return modelCounter.Stddev{Duration: agg.Options.Duration}, nil
	case "rate":
		return modelCounter.Rate{Duration: agg.Options.Duration, Normalize: agg.Options.Normalize}, nil
	default:
		return nil, fmt.Errorf("invalid aggregate type: %v", agg.Options.AggType)
	}
}
