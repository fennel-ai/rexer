package math

import (
	"context"
	"sync"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(meanop{})
}

type meanop struct{}

func (a meanop) New(
	args value.Dict, bootargs map[string]interface{}, cache *sync.Map,
) (operators.Operator, error) {
	return meanop{}, nil
}

func (a meanop) Apply(_ context.Context, _ value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		elems := heads[0].(value.List)
		var sum value.Value
		count := 0
		for i, elem := range elems.Values() {
			if i == 0 {
				sum = elem
			} else {
				sum, err = sum.Op("+", elem)
				if err != nil {
					return nil
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
		Input([]value.Type{value.Types.ListOfNumbers})
}

var _ operators.Operator = meanop{}
