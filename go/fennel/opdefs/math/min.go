package math

import (
	"context"
	"fmt"
	"log"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	if err := operators.Register(minOp{}); err != nil {
		log.Fatalf("Failed to register math.min operator: %v", err)
	}
}

type minOp struct{}

func (m minOp) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return minOp{}, nil
}

func (m minOp) Apply(_ context.Context, _ operators.Kwargs, in operators.InputIter, out *value.List) error {
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
		var minVal value.Value = value.Nil
		for i, elem := range elems.Values() {
			if err = value.Types.Number.Validate(elem); err != nil {
				return fmt.Errorf("value [%s] is not a number", elem.String())
			}
			if i == 0 {
				minVal = elem
			} else {
				gt, err := minVal.Op(">", elem)
				if err != nil {
					return err
				}
				if gt.(value.Bool) {
					minVal = elem
				}
			}
		}
		out.Append(minVal)
	}
	return nil
}

func (m minOp) Signature() *operators.Signature {
	return operators.NewSignature("math", "min").
		ParamWithHelp("of", value.Types.Any, false, true, nil, "Take min of values in this field of input")
}

var _ operators.Operator = minOp{}
