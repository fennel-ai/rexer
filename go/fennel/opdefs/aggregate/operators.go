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

func (a AggValue) Apply(kwargs value.Dict, in operators.InputIter, out *value.Table) error {
	name := string(kwargs["name"].(value.String))
	aggname := string(kwargs["aggregate"].(value.String))

	for in.HasMore() {
		row, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		key := contextKwargs["key"]
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
	return operators.NewSignature(a, "aggregate", "addField").
		Param("name", value.Types.String, true, false, value.Nil).
		Param("aggregate", value.Types.String, true, false, value.Nil).
		Param("key", value.Types.Any, false, false, value.Nil)
}

var _ operators.Operator = AggValue{}
