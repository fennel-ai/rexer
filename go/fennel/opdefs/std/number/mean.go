package number

import (
	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(meanop{})
}

type meanop struct{}

func (a meanop) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return meanop{}, nil
}

func (a meanop) Apply(_ value.Dict, in operators.InputIter, out *value.List) error {
	sum, num := float64(0), int64(0)

	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		this, _ := extract(heads[0])
		sum += this
		num += 1
	}
	var ret float64
	if num == 0 {
		ret = 0
	} else {
		ret = sum / float64(num)
	}
	return out.Append(value.Double(ret))
}

func (a meanop) Signature() *operators.Signature {
	return operators.NewSignature("std", "mean").
		Input([]value.Type{value.Types.Number})
}

var _ operators.Operator = meanop{}
