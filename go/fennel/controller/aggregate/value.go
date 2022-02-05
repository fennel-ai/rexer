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
	"fennel/tier"
	"fmt"
)

func Value(tier tier.Tier, aggtype ftypes.AggType, name ftypes.AggName, key value.Value) (value.Value, error) {
	agg, err := Retrieve(tier, aggtype, name)
	if err != nil {
		return value.Nil, err
	}
	return routeValue(tier, agg, key)
}

func Update(tier tier.Tier, agg aggregate.Aggregate) error {
	point, err := checkpoint.Get(tier, agg.Type, agg.Name)
	if err != nil {
		return err
	}
	actions, err := action.Fetch(tier, libaction.ActionFetchRequest{MinActionID: point})
	if err != nil {
		return err
	}
	table, err := transformActions(tier, actions, agg.Query)
	if err != nil {
		return err
	}
	if err = routeUpdate(tier, agg.Name, agg.Type, table); err != nil {
		return err
	}
	last := actions[len(actions)-1]
	return checkpoint.Set(tier, agg.Type, agg.Name, last.ActionID)
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

func routeUpdate(tier tier.Tier, aggname ftypes.AggName, aggtype ftypes.AggType, table value.Table) error {
	switch aggtype {
	case "rolling_counter":
		return counter.Update(tier, aggname, table)
	case "timeseries_counter":
		return counter.Update(tier, aggname, table)
	default:
		return fmt.Errorf("invalid aggregator type")
	}
}

func routeValue(tier tier.Tier, agg aggregate.Aggregate, key value.Value) (value.Value, error) {
	switch agg.Type {
	case "rolling_counter":
		return counter.RollingValue(tier, agg, key)
	case "timeseries_counter":
		return counter.TimeseriesValue(tier, agg, key)
	case "stream":
		return streamValue(tier, agg, key)
	default:
		return value.Nil, fmt.Errorf("invalid aggregate type: %v", agg.Type)
	}
}
