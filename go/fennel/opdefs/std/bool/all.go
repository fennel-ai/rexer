package bool

import (
	"context"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(allop{})
}

type allop struct{}

func (a allop) New(
	args *value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return allop{}, nil
}

func (a allop) Apply(_ context.Context, kwargs *value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		res := true
		vList := heads[0].(value.List)
		for _, v := range vList.Values() {
			if !v.(value.Bool) {
				res = false
				break
			}
		}
		out.Append(value.Bool(res))
	}
	return nil
}

func (a allop) Signature() *operators.Signature {
	return operators.NewSignature("std", "all").Input([]value.Type{value.Types.ListOfBools})
}

var _ operators.Operator = allop{}
