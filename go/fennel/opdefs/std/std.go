package std

import (
	"context"
	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	ops := []operators.Operator{
		FilterOperator{},
		TakeOperator{},
		ExplodeOperator{},
		SortOperator{},
		ShuffleOperator{},
		FlattenOperator{},
	}
	for _, op := range ops {
		if err := operators.Register(op); err != nil {
			panic(err)
		}
	}
}

type FilterOperator struct{}

func (f FilterOperator) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return FilterOperator{}, nil
}

func (f FilterOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "filter").
		ParamWithHelp("where", value.Types.Bool, false, false, value.Bool(false), "ContextKwargs: Expr that evaluates to a boolean.  If true, the row is included in the output.")
}

func (f FilterOperator) Apply(_ context.Context, _ operators.Kwargs, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		row := heads[0]
		v, _ := contextKwargs.Get("where")
		where := v.(value.Bool)
		if where {
			out.Append(row)
		}
	}
	return nil
}

type TakeOperator struct{}

func (f TakeOperator) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return TakeOperator{}, nil
}

func (f TakeOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "take").
		Param("limit", value.Types.Int, true, false, value.Nil)
}

func (f TakeOperator) Apply(_ context.Context, staticKwargs operators.Kwargs, in operators.InputIter, out *value.List) error {
	v, _ := staticKwargs.Get("limit")
	limit := v.(value.Int)
	taken := 0
	for in.HasMore() && taken < int(limit) {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		row := heads[0]
		out.Append(row)
		taken += 1
	}
	return nil
}
