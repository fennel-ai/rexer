package math

import (
	"context"
	"sync"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(minOp{})
}

type minOp struct{}

func (m minOp) New(args value.Dict, bootargs map[string]interface{}, cache *sync.Map) (operators.Operator, error) {
	return minOp{}, nil
}

func (m minOp) Apply(_ context.Context, _ value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		elems := heads[0].(value.List)
		var minVal value.Value
		minVal = value.Nil
		for i, elem := range elems.Values() {
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
		Input([]value.Type{value.Types.ListOfNumbers})
}

var _ operators.Operator = minOp{}
