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
		if elems.Len() == 0 {
			out.Append(value.Int(0))
			continue
		}
		maxVal, _ := elems.At(0)
		for i := 1; i < elems.Len(); i++ {
			v, _ := elems.At(i)
			more, err := maxVal.Op("<", v)
			if err != nil {
				return err
			}
			if more.(value.Bool) {
				maxVal = v
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
