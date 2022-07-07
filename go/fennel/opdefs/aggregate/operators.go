package aggregate

import (
	"context"
	"fennel/lib/arena"
	"fmt"
	"log"

	"fennel/controller/aggregate"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	aggregate2 "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/tier"
)

func init() {
	if err := operators.Register(AggValue{}); err != nil {
		log.Fatalf("Failed to register std.aggregate operator: %v", err)
	}
}

type AggValue struct {
	tier tier.Tier
}

func (a AggValue) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	tr, err := bootarg.GetTier(bootargs)
	if err != nil {
		return nil, err
	}
	return AggValue{tr}, nil
}

func (a AggValue) Apply(ctx context.Context, staticKwargs operators.Kwargs, in operators.InputIter, outs *value.List) error {
	var reqs []aggregate2.GetAggValueRequest
	var rows []value.Value = arena.Values.Alloc(0, 256)
	defer arena.Values.Free(rows)

	for in.HasMore() {
		heads, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}

		gk, ok := contextKwargs.Get("groupkey")
		if !ok || gk == value.Nil {
			gk = heads[0]
		}

		req := aggregate2.GetAggValueRequest{
			AggName: ftypes.AggName(contextKwargs.GetUnsafe("name").(value.String)),
			Key:     gk,
			Kwargs:  contextKwargs.GetUnsafe("kwargs").(value.Dict),
		}
		reqs = append(reqs, req)
		rows = append(rows, heads[0])
	}
	res, err := aggregate.BatchValue(ctx, a.tier, reqs)
	if err != nil {
		return err
	}
	field := string(staticKwargs.GetUnsafe("field").(value.String))
	outs.Grow(len(rows))
	for i, row := range rows {
		var out value.Value
		if len(field) > 0 {
			var d value.Dict
			d, ok := row.(value.Dict)
			if !ok {
				return fmt.Errorf("when setting a field, operands for aggregator are required to be dicts, please convert them to a dict using map")
			}
			d.Set(field, res[i])
			out = d
		} else {
			out = res[i]
		}
		outs.Append(out)
	}
	return nil
}

func (a AggValue) Signature() *operators.Signature {
	return operators.NewSignature("std", "aggregate").
		Input([]value.Type{value.Types.Any}).
		ParamWithHelp("field", value.Types.String, true, true, value.String(""), "StaticKwarg: String param that is used as key post evaluation of this operator").
		ParamWithHelp("name", value.Types.String, false, false, value.Nil, "ContextKwarg: Expr of type string when evaluated provides the name of the aggregate to be used.").
		ParamWithHelp("groupkey", value.Types.Any, false, true, value.Nil, "ContextKwarg: Expr that is evaluated to provide the lookup/groupkey in the aggregate.").
		ParamWithHelp("kwargs", value.Types.Dict, false, false, value.Dict{}, "ContextKwarg: Dict of key/value pairs that are passed to the aggregate.")
}

var _ operators.Operator = AggValue{}
