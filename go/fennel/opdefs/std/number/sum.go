package number

import (
	"context"
	"sync"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(adder{})
}

type adder struct{}

func (a adder) New(
	args value.Dict, bootargs map[string]interface{}, cache *sync.Map,
) (operators.Operator, error) {
	return adder{}, nil
}

func (a adder) Apply(_ context.Context, _ value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		elems := heads[0].(value.List)
		sum := kwargs.GetUnsafe("start")
		for _, elem := range elems.Values() {
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
	return operators.NewSignature("std", "sum").
		Input([]value.Type{value.Types.ListOfNumbers}).
		Param("start", value.Types.Number, false, true, value.Int(0))
}

var _ operators.Operator = adder{}
