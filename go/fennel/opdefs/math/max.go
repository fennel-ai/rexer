package math

import (
	"context"
	"sync"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(maxOp{})
}

type maxOp struct{}

func (m maxOp) New(args value.Dict, bootargs map[string]interface{}, cache *sync.Map) (operators.Operator, error) {
	return maxOp{}, nil
}

func (m maxOp) Apply(_ context.Context, _ value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		elems := heads[0].(value.List)
		var maxVal value.Value
		maxVal = value.Nil
		for i, elem := range elems.Values() {
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
		Input([]value.Type{value.Types.ListOfNumbers})
}
