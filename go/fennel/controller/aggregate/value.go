package aggregate

import (
	"fennel/controller/action"
	"fennel/controller/counter"
	"fennel/engine/ast"
	"fennel/engine/interpreter"
	"fennel/engine/interpreter/bootarg"
	libaction "fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/checkpoint"
	modelCounter "fennel/model/counter"
	_ "fennel/opdefs"
	"fennel/tier"
	"fmt"
)

func Value(tier tier.Tier, name ftypes.AggName, key value.Value) (value.Value, error) {
	agg, err := Retrieve(tier, name)
	if err != nil {
		return value.Nil, err
	}
	histogram, err := toHistogram(agg)
	if err != nil {
		return nil, err
	}
	return counter.Value(tier, agg.Name, key, histogram)
}

func Update(tier tier.Tier, agg aggregate.Aggregate) error {
	point, err := checkpoint.Get(tier, ftypes.AggType(agg.Options.AggType), agg.Name)
	if err != nil {
		return err
	}
	actions, err := action.Fetch(tier, libaction.ActionFetchRequest{MinActionID: point})
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
	if err = counter.Update(tier, agg.Name, table, histogram); err != nil {
		return err
	}
	last := actions[len(actions)-1]
	return checkpoint.Set(tier, ftypes.AggType(agg.Options.AggType), agg.Name, last.ActionID)
}

//============================
// Private helpers below
//============================

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
	default:
		return nil, fmt.Errorf("invalid aggregate type: %v", agg.Options.AggType)
	}
}
