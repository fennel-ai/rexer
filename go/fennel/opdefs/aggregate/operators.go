package aggregate

import (
	"context"
	"sync"

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

func (a AggValue) New(
	args value.Dict, bootargs map[string]interface{}, cache *sync.Map,
) (operators.Operator, error) {
	tr, err := bootarg.GetTier(bootargs)
	if err != nil {
		return nil, err
	}
	return AggValue{tr}, nil
}

func (a AggValue) Apply(ctx context.Context, kwargs value.Dict, in operators.InputIter, outs *value.List) error {
	var reqs []aggregate2.GetAggValueRequest
	var rows []value.Value
	for in.HasMore() {
		heads, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		req := aggregate2.GetAggValueRequest{
			AggName: ftypes.AggName(get(contextKwargs, "name").(value.String)),
			Key:     get(contextKwargs, "groupkey"),
			Kwargs:  get(contextKwargs, "kwargs").(value.Dict),
		}
		reqs = append(reqs, req)
		rows = append(rows, heads[0])
	}
	res, err := aggregate.BatchValue(ctx, a.tier, reqs)
	if err != nil {
		return err
	}
	field := string(get(kwargs, "field").(value.String))
	for i, row := range rows {
		var out value.Value
		if len(field) > 0 {
			d := row.(value.Dict)
			d.Set(field, res[i])
			out = d
		} else {
			out = res[i]
		}
		outs.Append(out)
	}
	return nil
}

func get(d value.Dict, k string) value.Value {
	ret, _ := d.Get(k)
	return ret
}

func (a AggValue) Signature() *operators.Signature {
	return operators.NewSignature("std", "aggregate").
		Input([]value.Type{value.Types.Any}).
		ParamWithHelp("field", value.Types.String, true, true, value.String(""), "StaticKwarg: String param that is used as key post evaluation of this operator").
		ParamWithHelp("name", value.Types.String, false, false, value.Nil, "ContextKwarg: Expr of type string when evaluated provides the name of the aggregate to be used.").
		ParamWithHelp("groupkey", value.Types.Any, false, false, value.Nil, "ContextKwarg: Expr that is evaluated to provide the lookup/groupkey in the aggregate.").
		ParamWithHelp("kwargs", value.Types.Dict, false, false, value.Dict{}, "ContextKwarg: Dict of key/value pairs that are passed to the aggregate.")
}

var _ operators.Operator = AggValue{}
