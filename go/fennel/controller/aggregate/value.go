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
	counter2 "fennel/model/counter"
	_ "fennel/opdefs"
	"fennel/tier"
	"fmt"
)

func Value(tier tier.Tier, name ftypes.AggName, key value.Value) (value.Value, error) {
	agg, err := Retrieve(tier, name)
	if err != nil {
		return value.Nil, err
	}
	return routeValue(tier, agg, key)
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
	if err = routeUpdate(tier, agg, table); err != nil {
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

func routeUpdate(tier tier.Tier, agg aggregate.Aggregate, table value.Table) error {
	aggType := ftypes.AggType(agg.Options.AggType)
	switch aggType {
	case "rolling_counter":
		return counter.Update(tier, agg.Name, table, counter2.RollingCounter{})
	case "timeseries_counter":
		return counter.Update(tier, agg.Name, table, counter2.TimeseriesCounter{
			Window: agg.Options.Window, Limit: agg.Options.Limit,
		})
	default:
		return fmt.Errorf("invalid aggregator type")
	}
}

func routeValue(tier tier.Tier, agg aggregate.Aggregate, key value.Value) (value.Value, error) {
	switch agg.Options.AggType {
	case "rolling_counter":
		return counter.Value(tier, agg, key, counter2.RollingCounter{Duration: agg.Options.Duration})
	case "timeseries_counter":
		return counter.Value(tier, agg, key, counter2.TimeseriesCounter{
			Window: agg.Options.Window, Limit: agg.Options.Limit,
		})
	case "stream":
		return streamValue(tier, agg, key)
	default:
		return value.Nil, fmt.Errorf("invalid aggregate type: %v", agg.Options.AggType)
	}
}
