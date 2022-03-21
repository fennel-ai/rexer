package std

import (
	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	ops := []operators.Operator{
		FilterOperator{},
		TakeOperator{},
		AddField{},
		ExplodeOperator{},
		SortOperator{},
		ShuffleOperator{},
		UniqueOperator{},
	}
	for _, op := range ops {
		if err := operators.Register(op); err != nil {
			panic(err)
		}
	}
}

type FilterOperator struct{}

func (f FilterOperator) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return FilterOperator{}, nil
}

func (f FilterOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "filter", false).
		Param("where", value.Types.Bool, false, false, value.Bool(false))
}

func (f FilterOperator) Apply(_ value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		row, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		v, _ := contextKwargs.Get("where")
		where := v.(value.Bool)
		if where {
			out.Append(row)
		}
	}
	return nil
}

type TakeOperator struct{}

func (f TakeOperator) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return TakeOperator{}, nil
}

func (f TakeOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "take", false).
		Param("limit", value.Types.Int, true, false, value.Nil)
}

func (f TakeOperator) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	v, _ := staticKwargs.Get("limit")
	limit := v.(value.Int)
	taken := 0
	for in.HasMore() && taken < int(limit) {
		row, _, err := in.Next()
		if err != nil {
			return err
		}
		out.Append(row)
		taken += 1
	}
	return nil
}

type AddField struct{}

var _ operators.Operator = AddField{}

func (op AddField) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return AddField{}, nil
}

func (op AddField) Signature() *operators.Signature {
	return operators.NewSignature("std", "addField", true).
		Param("name", value.Types.String, true, false, value.Nil).
		Param("value", value.Types.Any, false, false, value.Nil).
		Input(value.Types.Dict)
}

func (op AddField) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	n, _ := staticKwargs.Get("name")
	name := string(n.(value.String))
	for in.HasMore() {
		rowVal, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		row := rowVal.(value.Dict)
		v, _ := contextKwargs.Get("value")
		row.Set(name, v)
		out.Append(row)
	}
	return nil
}
