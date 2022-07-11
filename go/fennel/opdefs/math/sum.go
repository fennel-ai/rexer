package math

import (
	"context"
	"fmt"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	err := operators.Register(adder{})
	if err != nil {
		panic(err)
	}
}

type adder struct{}

func (a adder) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return adder{}, nil
}

func (a adder) Apply(_ context.Context, _ operators.Kwargs, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		of, _ := kwargs.Get("of")
		var elems value.List
		if of == nil {
			elems = heads[0].(value.List)
		} else {
			elems = of.(value.List)
		}
		var sum value.Value
		sum = kwargs.GetUnsafe("zero")
		for _, elem := range elems.Values() {
			if err = value.Types.Number.Validate(elem); err != nil {
				return fmt.Errorf("value [%s] is not a number", elem.String())
			}
			sum, err = sum.Op("+", elem)
			if err != nil {
				return err
			}
		}
		out.Append(sum)
	}
	return nil
}

func (a adder) Signature() *operators.Signature {
	return operators.NewSignature("math", "sum").
		ParamWithHelp("zero", value.Types.Number, false, true, value.Int(0), "The zero value for the sum").
		ParamWithHelp("of", value.Types.Any, false, true, nil, "Take sum of values in this field of input")
}

var _ operators.Operator = adder{}
