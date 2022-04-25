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
		if elems.Len() == 0 {
			out.Append(value.Int(0))
			continue
		}
		minVal, _ := elems.At(0)
		for i := 1; i < elems.Len(); i++ {
			v, _ := elems.At(i)
			less, err := minVal.Op(">", v)
			if err != nil {
				return err
			}
			if less.(value.Bool) {
				minVal = v
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
