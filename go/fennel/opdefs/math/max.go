package math

import (
	"context"
	"fmt"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(maxOp{})
}

type maxOp struct{}

func (m maxOp) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return maxOp{}, nil
}

func (m maxOp) Apply(_ context.Context, _ value.Dict, in operators.InputIter, out *value.List) error {
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
		var maxVal value.Value = value.Nil
		for i, elem := range elems.Values() {
			if err = value.Types.Number.Validate(elem); err != nil {
				return fmt.Errorf("value [%s] is not a number", elem.String())
			}
			if i == 0 {
				maxVal = elem
			} else {
				lt, err := maxVal.Op("<", elem)
				if err != nil {
					return err
				}
				if lt.(value.Bool) {
					maxVal = elem
				}
			}
		}
		out.Append(maxVal)
	}
	return nil
}

func (m maxOp) Signature() *operators.Signature {
	return operators.NewSignature("math", "max").
		ParamWithHelp("of", value.Types.Any, false, true, nil, "Take max of values in this field of input")
}
