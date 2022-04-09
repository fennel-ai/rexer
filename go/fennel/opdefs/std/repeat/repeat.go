package repeat

import (
	"fennel/engine/operators"
	"fennel/lib/value"
	"fmt"
)

func init() {
	if err := operators.Register(repeater{}); err != nil {
		panic(err)
	}
}

type repeater struct{}

func (r repeater) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return repeater{}, nil
}

func (r repeater) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		count := int64(kwargs.GetUnsafe("count").(value.Int))
		if count < 0 {
			return fmt.Errorf("repeat: negative repeat count")
		}
		for i := int64(0); i < count; i++ {
			if i == 0 {
				out.Append(heads[0])
			} else {
				out.Append(heads[0].Clone())
			}
		}
	}
	return nil
}

func (r repeater) Signature() *operators.Signature {
	return operators.NewSignature("std", "repeat").
		ParamWithHelp("count", value.Types.Int, false, false, nil,
			"number of times to repeat each element of the input",
		)
}

var _ operators.Operator = repeater{}
