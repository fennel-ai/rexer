package std

import (
	"fmt"
	"sort"

	"fennel/engine/operators"
	"fennel/lib/value"
)

type SortOperator struct{}

var _ operators.Operator = SortOperator{}

type sortableRows struct {
	data value.List
	keys []float64
	desc value.Bool
}

func (s sortableRows) Len() int {
	return len(s.data)
}

func (s sortableRows) Less(i, j int) bool {
	if s.desc {
		return s.keys[j] < s.keys[i]
	} else {
		return s.keys[i] < s.keys[j]
	}
}

func (s sortableRows) Swap(i, j int) {
	s.data[i], s.data[j] = s.data[j], s.data[i]
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
}

var _ sort.Interface = sortableRows{}

func (op SortOperator) New(_ value.Dict, _ map[string]interface{}) (operators.Operator, error) {
	return SortOperator{}, nil
}

func (op SortOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "sort", true).
		Input(value.Types.Dict).
		Param("by", value.Types.Number, false, false, value.Nil).
		Param("desc", value.Types.Bool, true, true, value.Bool(false))
}

func (op SortOperator) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	rows := sortableRows{}
	for in.HasMore() {
		row, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		var v float64
		key := contextKwargs["by"]
		switch key := key.(type) {
		case value.Int:
			v = float64(key)
		case value.Double:
			v = float64(key)
		default:
			expType := op.Signature().ContextKwargs["by"].Type
			return fmt.Errorf("value of context kwarg 'by' is not of type '%s': %s", expType, expType.Validate(key))
		}
		rows.data = append(rows.data, row)
		rows.keys = append(rows.keys, v)
	}
	rows.desc = staticKwargs["desc"].(value.Bool)
	sort.Stable(rows)
	for _, row := range rows.data {
		if err := out.Append(row); err != nil {
			return err
		}
	}
	return nil
}
