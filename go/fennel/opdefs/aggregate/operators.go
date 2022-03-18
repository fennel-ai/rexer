package aggregate

import (
	"context"

	"fennel/controller/aggregate"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	aggregate2 "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/tier"
)

func init() {
	ops := []operators.Operator{AggValue{}}
	for _, op := range ops {
		if err := operators.Register(op); err != nil {
			panic(err)
		}
	}
}

type AggValue struct {
	tier tier.Tier
}

func (a AggValue) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	tr, err := bootarg.GetTier(bootargs)
	if err != nil {
		return nil, err
	}
	return AggValue{tr}, nil
}

func (a AggValue) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	name := string(kwargs["name"].(value.String))
	aggname := string(kwargs["aggregate"].(value.String))

	var reqs []aggregate2.GetAggValueRequest
	var rows []value.Dict
	for in.HasMore() {
		rowVal, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		req := aggregate2.GetAggValueRequest{
			AggName: ftypes.AggName(aggname),
			Key:     contextKwargs["groupkey"],
			Kwargs:  contextKwargs["kwargs"].(value.Dict),
		}
		reqs = append(reqs, req)
		row := rowVal.(value.Dict)
		rows = append(rows, row)
	}
	res, err := aggregate.BatchValue(context.TODO(), a.tier, reqs)
	if err != nil {
		return err
	}
	for i, row := range rows {
		row[name] = res[i]
		if err = out.Append(row); err != nil {
			return err
		}
	}
	return nil
}

func (a AggValue) Signature() *operators.Signature {
	return operators.NewSignature("aggregate", "addField", true).
		Input(value.Types.Dict).
		Param("name", value.Types.String, true, false, value.Nil).
		Param("aggregate", value.Types.String, true, false, value.Nil).
		Param("groupkey", value.Types.Any, false, false, value.Nil).
		Param("kwargs", value.Types.Dict, false, true, value.Dict{})
}

var _ operators.Operator = AggValue{}
