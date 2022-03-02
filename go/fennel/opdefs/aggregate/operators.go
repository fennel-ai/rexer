package aggregate

import (
	"context"

	"fennel/controller/aggregate"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/tier"
)

type AggValue struct {
	tier tier.Tier
}

func (a AggValue) Init(_ value.Dict, bootargs map[string]interface{}) error {
	var err error
	if a.tier, err = bootarg.GetTier(bootargs); err != nil {
		return err
	}
	return nil
}

func (a AggValue) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	name := string(kwargs["name"].(value.String))
	aggname := string(kwargs["aggregate"].(value.String))

	for in.HasMore() {
		rowVal, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		row := rowVal.(value.Dict)
		key := contextKwargs["groupkey"]
		row[name], err = aggregate.Value(context.TODO(), a.tier, ftypes.AggName(aggname), key)
		if err != nil {
			return err
		}
		if err = out.Append(row); err != nil {
			return err
		}
	}
	return nil
}

func (a AggValue) Signature() *operators.Signature {
	return operators.NewSignature("aggregate", "addField").
		Input(value.Types.Dict).
		Param("name", value.Types.String, true, false, value.Nil).
		Param("aggregate", value.Types.String, true, false, value.Nil).
		Param("groupkey", value.Types.Any, false, false, value.Nil)
}

var _ operators.Operator = AggValue{}
