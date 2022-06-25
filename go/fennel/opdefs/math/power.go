package math

import (
	"context"
	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(powerOp{})
}

type powerOp struct{}

func (m powerOp) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return powerOp{}, nil
}

func (m powerOp) Apply(_ context.Context, _ operators.Kwargs, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		of, _ := kwargs.Get("of")
		var elem value.Value
		if of == nil {
			elem = heads[0]
		} else {
			elem = of
		}
		ret, err := elem.Op("^", kwargs.GetUnsafe("power"))
		if err != nil {
			return err
		}
		out.Append(ret)
	}
	return nil
}

func (m powerOp) Signature() *operators.Signature {
	return operators.NewSignature("math", "power").
		ParamWithHelp("of", value.Types.Any, false, true, nil, "Take the power of values in this field of input").
		ParamWithHelp("power", value.Types.Any, false, false, nil, "The power to raise the values to")
}

var _ operators.Operator = powerOp{}
