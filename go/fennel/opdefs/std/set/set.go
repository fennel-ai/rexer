package set

import (
	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(setOperator{})
}

type setOperator struct{}

var _ operators.Operator = setOperator{}

func (op setOperator) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return setOperator{}, nil
}

func (op setOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "set").
		Param("field", value.Types.String, false, false, value.Nil).
		Param("value", value.Types.Any, false, false, value.Nil).
		Input([]value.Type{value.Types.Dict})
}

func (op setOperator) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		row := heads[0].(value.Dict)
		v, _ := contextKwargs.Get("value")
		k, _ := contextKwargs.Get("field")
		row.Set(string(k.(value.String)), v)
		out.Append(row)
	}
	return nil
}
