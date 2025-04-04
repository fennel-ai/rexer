package zip

import (
	"context"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	if err := operators.Register(zipper{}); err != nil {
		panic(err)
	}
}

type zipper struct{}

func (z zipper) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return zipper{}, nil
}

func (z zipper) Apply(_ context.Context, _ operators.Kwargs, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		v, _, err := in.Next()
		if err != nil {
			return err
		}
		zipped := value.NewList(v...)
		out.Append(zipped)
	}
	return nil
}

func (z zipper) Signature() *operators.Signature {
	return operators.NewSignature("std", "zip").Input(nil)
}

var _ operators.Operator = zipper{}
