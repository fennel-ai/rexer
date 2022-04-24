package number

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
		var sum value.Value = value.Int(0)
		num := 0
		for _, elem := range elems.Values() {
			sum, err = sum.Op("+", elem)
			if err != nil {
				return nil
			}
			num++
		}
		if num == 0 {
			out.Append(value.Int(0))
		} else {
			mean, err := sum.Op("/", value.Int(num))
			if err != nil {
				return err
			}
			out.Append(mean)
		}
	}
	return nil
}

func (a meanop) Signature() *operators.Signature {
	return operators.NewSignature("std", "mean").
		Input([]value.Type{value.Types.ListOfNumbers})
}

var _ operators.Operator = meanop{}
