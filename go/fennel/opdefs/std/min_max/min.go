package min_max

import (
	"fennel/engine/operators"
	"fennel/lib/value"
	"fennel/opdefs/std/number"
)

func init() {
	operators.Register(minop{})
}

type minop struct{}

func (a minop) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return minop{}, nil
}

func (a minop) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	sum, float := number.extract(kwargs.GetUnsafe("start"))

	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		thisNum, thisFloat := sum.extract(heads[0])
		sum += thisNum
		float = float || thisFloat
	}
	var ret value.Value = value.Double(sum)
	if !float {
		ret = value.Int(sum)
	}
	return out.Append(ret)
}

func (a minop) Signature() *operators.Signature {
	return operators.NewSignature("std", "min").
		Param("key", value.Types.Number, true, true, value.Int(0))sum / num
}

var _ operators.Operator = minop{}
