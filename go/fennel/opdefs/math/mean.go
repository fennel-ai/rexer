package math

import (
	"context"
	"fmt"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(meanop{})
}

type meanop struct{}

func (a meanop) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return meanop{}, nil
}

func (a meanop) Apply(_ context.Context, _ operators.Kwargs, in operators.InputIter, out *value.List) error {
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
		count := 0
		for i, elem := range elems.Values() {
			if err = value.Types.Number.Validate(elem); err != nil {
				return fmt.Errorf("value [%s] is not a number", elem.String())
			}
			if i == 0 {
				sum = elem
			} else {
				sum, err = sum.Op("+", elem)
				if err != nil {
					return err
				}
			}
			count++
		}
		if count == 0 {
			out.Append(value.Nil)
		} else {
			mean, err := sum.Op("/", value.Int(count))
			if err != nil {
				return err
			}
			out.Append(mean)
		}
	}
	return nil
}

func (a meanop) Signature() *operators.Signature {
	return operators.NewSignature("math", "mean").
		ParamWithHelp("of", value.Types.Any, false, true, nil, "Take mean of values in this field of input")
}

var _ operators.Operator = meanop{}
