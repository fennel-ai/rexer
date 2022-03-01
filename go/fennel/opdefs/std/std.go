package std

import (
	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	ops := []operators.Operator{FilterOperator{}, TakeOperator{}, AddField{}}
	for _, op := range ops {
		if err := operators.Register(op); err != nil {
			panic(err)
		}
	}
}

type FilterOperator struct{}

func (f FilterOperator) Init(_ value.Dict, _ map[string]interface{}) error {
	return nil
}

func (f FilterOperator) Signature() *operators.Signature {
	return operators.NewSignature(f, "std", "filter").
		Param("where", value.Types.Bool, false, false, value.Bool(false))
}

func (f FilterOperator) Apply(_ value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		row, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		where := contextKwargs["where"].(value.Bool)
		if where {
			out.Append(row)
		}
	}
	return nil
}

type TakeOperator struct{}

func (f TakeOperator) Init(_ value.Dict, _ map[string]interface{}) error {
	return nil
}

func (f TakeOperator) Signature() *operators.Signature {
	return operators.NewSignature(f, "std", "take").
		Param("limit", value.Types.Int, true, false, value.Nil)
}

func (f TakeOperator) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	limit := staticKwargs["limit"].(value.Int)
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

func (op AddField) Init(_ value.Dict, _ map[string]interface{}) error {
	return nil
}

func (op AddField) Signature() *operators.Signature {
	return operators.NewSignature(op, "std", "addField").
		Param("name", value.Types.String, true, false, value.Nil).
		Param("value", value.Types.Any, false, false, value.Nil).
		Input(value.Types.Dict)
}

func (op AddField) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	name := string(staticKwargs["name"].(value.String))
	for in.HasMore() {
		rowVal, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		row := rowVal.(value.Dict)
		row[name] = contextKwargs["value"]
		out.Append(row)
	}
	return nil
}
