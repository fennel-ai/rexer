package bool

import (
	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(anyop{})
}

type anyop struct{}

func (a anyop) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return anyop{}, nil
}

func (a anyop) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		if heads[0].(value.Bool) {
			return out.Append(value.Bool(true))
		}
	}
	return out.Append(value.Bool(false))
}

func (a anyop) Signature() *operators.Signature {
	return operators.NewSignature("std", "any").Input([]value.Type{value.Types.Bool})
}

var _ operators.Operator = anyop{}
