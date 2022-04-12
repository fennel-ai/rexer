package bool

import (
	"context"
	"fennel/engine/operators"
	"fennel/lib/value"
	"sync"
)

func init() {
	operators.Register(allop{})
}

type allop struct{}

func (a allop) New(
	args value.Dict, bootargs map[string]interface{}, cache *sync.Map,
) (operators.Operator, error) {
	return allop{}, nil
}

func (a allop) Apply(_ context.Context, kwargs value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		if !heads[0].(value.Bool) {
			out.Append(value.Bool(false))
			return nil
		}
	}
	out.Append(value.Bool(true))
	return nil
}

func (a allop) Signature() *operators.Signature {
	return operators.NewSignature("std", "all").Input([]value.Type{value.Types.Bool})
}

var _ operators.Operator = allop{}
