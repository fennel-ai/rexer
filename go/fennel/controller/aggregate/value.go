package aggregate

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

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
	_ "fennel/opdefs"
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

func Update(ctx context.Context, tier tier.Tier, consumer kafka.FConsumer, agg aggregate.Aggregate) error {
	actions, err := readActions(ctx, consumer)
	if err != nil {
		return err
	}
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
	// TODO: currently our kafka is committing things by default
	// and so when we commit, it has nothing to commit. Ideally we will fix that
	// and start returning the error of commit call itself
	consumer.Commit()
	return nil
}

//============================
// Private helpers below
//============================

func readActions(ctx context.Context, consumer kafka.FConsumer) ([]libaction.Action, error) {
	msgs, err := consumer.ReadBatch(ctx, 10000, time.Second*5)
	if err != nil {
		return nil, err
	}
	actions := make([]libaction.Action, len(msgs))
	for i := range msgs {
		var pa libaction.ProtoAction
		if err = proto.Unmarshal(msgs[i], &pa); err != nil {
			return nil, err
		}
		if actions[i], err = libaction.FromProtoAction(&pa); err != nil {
			return nil, err
		}
	}
	return actions, nil
}

func transformActions(tier tier.Tier, actions []libaction.Action, query ast.Ast) (value.Table, error) {
	interpreter, err := loadInterpreter(tier, actions)
	if err != nil {
		return value.Table{}, err
	}
	result, err := query.AcceptValue(interpreter)
	if err != nil {
		return value.Table{}, err
	}
	table, ok := result.(value.Table)
	if !ok {
		return value.Table{}, fmt.Errorf("query did not transform actions into a table")
	}
	return table, nil
}

func loadInterpreter(tier tier.Tier, actions []libaction.Action) (interpreter.Interpreter, error) {
	bootargs := bootarg.Create(tier)
	ret := interpreter.NewInterpreter(bootargs)
	table, err := libaction.ToTable(actions)
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
	case "rolling_counter":
		return modelCounter.RollingCounter{Duration: agg.Options.Duration}, nil
	case "timeseries_counter":
		return modelCounter.TimeseriesCounter{
			Window: agg.Options.Window, Limit: agg.Options.Limit,
		}, nil
	case "rolling_average":
		return modelCounter.RollingAverage{Duration: agg.Options.Duration}, nil
	case "stream":
		return modelCounter.Stream{Duration: agg.Options.Duration}, nil
	default:
		return nil, fmt.Errorf("invalid aggregate type: %v", agg.Options.AggType)
	}
}
