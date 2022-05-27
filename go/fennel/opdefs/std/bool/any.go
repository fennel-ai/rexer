package bool

import (
	"context"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(anyop{})
}

type anyop struct{}

func (a anyop) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return anyop{}, nil
}

func (a anyop) Apply(_ context.Context, kwargs value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		res := false
		vList := heads[0].(value.List)
		for _, v := range vList.Values() {
			if v.(value.Bool) {
				res = true
				break
			}
		}
		out.Append(value.Bool(res))
	}
	return nil
}

func (a anyop) Signature() *operators.Signature {
	return operators.NewSignature("std", "any").Input([]value.Type{value.Types.ListOfBools})
}

var _ operators.Operator = anyop{}
