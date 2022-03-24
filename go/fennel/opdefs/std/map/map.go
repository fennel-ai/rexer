package _map

import (
	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(mapper{})
}

type mapper struct {
}

func (m mapper) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return mapper{}, nil
}

func (m mapper) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		_, context, err := in.Next()
		if err != nil {
			return err
		}
		if err = out.Append(context.GetUnsafe("to")); err != nil {
			return err
		}
	}
	return nil
}

func (m mapper) Signature() *operators.Signature {
	return operators.NewSignature("std", "map").
		Param("to", value.Types.Any, false, false, nil)
}

var _ operators.Operator = mapper{}
