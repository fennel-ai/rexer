package _map

import (
	"context"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(mapper{})
}

type mapper struct {
}

func (m mapper) New(
	args *value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return mapper{}, nil
}

func (m mapper) Apply(_ context.Context, kwargs *value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		_, context, err := in.Next()
		if err != nil {
			return err
		}
		out.Append(context.GetUnsafe("to"))
	}
	return nil
}

func (m mapper) Signature() *operators.Signature {
	return operators.NewSignature("std", "map").
		ParamWithHelp("to", value.Types.Any, false, false, nil, "ContextKwargs:  Each value gets converted to this Expr").
		Input(nil)
}

var _ operators.Operator = mapper{}
