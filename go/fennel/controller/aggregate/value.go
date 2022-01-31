package aggregate

import (
	"fennel/controller/action"
	"fennel/controller/counter"
	"fennel/engine/ast"
	"fennel/engine/interpreter"
	"fennel/instance"
	libaction "fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/checkpoint"
	"fmt"
)

func Value(instance instance.Instance, aggtype ftypes.AggType, name ftypes.AggName, key value.Value) (value.Value, error) {
	agg, err := Retrieve(instance, aggtype, name)
	if err != nil {
		return value.Nil, err
	}
	return routeValue(instance, agg, key)
}

func Update(instance instance.Instance, agg aggregate.Aggregate) error {
	point, err := checkpoint.Get(instance, agg.Type, agg.Name)
	if err != nil {
		return err
	}
	actions, err := action.Fetch(instance, libaction.ActionFetchRequest{MinActionID: point})
	if err != nil {
		return err
	}
	table, err := transformActions(instance, actions, agg.Query)
	if err != nil {
		return err
	}
	if err = routeUpdate(instance, agg.Name, agg.Type, table); err != nil {
		return err
	}
	last := actions[len(actions)-1]
	return checkpoint.Set(instance, agg.Type, agg.Name, last.ActionID)
}

//============================
// Private helpers below
//============================

func transformActions(instance instance.Instance, actions []libaction.Action, query ast.Ast) (value.Table, error) {
	interpreter, err := loadInterpreter(instance, actions)
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

func loadInterpreter(instance instance.Instance, actions []libaction.Action) (interpreter.Interpreter, error) {
	bootargs := map[string]interface{}{
		"__instance__": instance,
	}
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

func routeUpdate(instance instance.Instance, aggname ftypes.AggName, aggtype ftypes.AggType, table value.Table) error {
	switch aggtype {
	case "rolling_counter":
		return counter.Update(instance, aggname, table)
	case "timeseries_counter":
		return counter.Update(instance, aggname, table)
	default:
		return fmt.Errorf("invalid aggregator type")
	}
}

func routeValue(instance instance.Instance, agg aggregate.Aggregate, key value.Value) (value.Value, error) {
	switch agg.Type {
	case "rolling_counter":
		return counter.RollingValue(instance, agg, key)
	case "timeseries_counter":
		return counter.TimeseriesValue(instance, agg, key)
	case "stream":
		return streamValue(instance, agg, key)
	default:
		return value.Nil, fmt.Errorf("invalid aggregate type: %v", agg.Type)
	}
}
