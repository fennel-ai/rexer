package number

import (
	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(adder{})
}

type adder struct{}

func (a adder) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return adder{}, nil
}

func (a adder) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	sum, float := extract(kwargs.GetUnsafe("start"))

	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		thisNum, thisFloat := extract(heads[0])
		sum += thisNum
		float = float || thisFloat
	}
	var ret value.Value = value.Double(sum)
	if !float {
		ret = value.Int(sum)
	}
	out.Append(ret)
	return nil
}

func extract(n value.Value) (float64, bool) {
	switch t := n.(type) {
	case value.Int:
		return float64(t), false
	case value.Double:
		return float64(t), true
	default:
		panic("this should not happen")
	}
}

func (a adder) Signature() *operators.Signature {
	return operators.NewSignature("std", "sum").
		Input([]value.Type{value.Types.Number}).
		Param("start", value.Types.Number, true, true, value.Int(0))
}

var _ operators.Operator = adder{}
