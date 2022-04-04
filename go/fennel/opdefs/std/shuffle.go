package std

import (
	"math/rand"
	"reflect"

	"fennel/engine/operators"
	"fennel/lib/value"
)

type ShuffleOperator struct{}

var _ operators.Operator = ShuffleOperator{}

func (op ShuffleOperator) New(_ value.Dict, _ map[string]interface{}) (operators.Operator, error) {
	return ShuffleOperator{}, nil
}

func (op ShuffleOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "shuffle").
		Input([]value.Type{value.Types.Dict})
}

func (op ShuffleOperator) Apply(_ value.Dict, in operators.InputIter, out *value.List) error {
	var rows []value.Value
	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		row := heads[0]
		rows = append(rows, row)
	}
	rand.Shuffle(len(rows), reflect.Swapper(rows))
	for _, row := range rows {
		out.Append(row)
	}
	return nil
}
