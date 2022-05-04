package math

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
		var sum value.Value
		sum = kwargs.GetUnsafe("zero")
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
	return operators.NewSignature("math", "sum").
		Input([]value.Type{value.Types.ListOfNumbers}).
		ParamWithHelp("zero", value.Types.Number, false, true, value.Int(0), "The zero value for the sum")
}

var _ operators.Operator = adder{}
