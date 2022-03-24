package std

import (
	"fmt"
	"sort"

	"fennel/engine/operators"
	"fennel/lib/value"
)

type SortOperator struct{}

var _ operators.Operator = SortOperator{}

func (op SortOperator) New(_ value.Dict, _ map[string]interface{}) (operators.Operator, error) {
	return SortOperator{}, nil
}

func (op SortOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "sort").
		Input([]value.Type{value.Types.Dict}).
		Param("by", value.Types.Number, false, false, value.Nil).
		Param("reverse", value.Types.Bool, true, true, value.Bool(false))
}

func (op SortOperator) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	type sortableRow struct {
		data value.Value
		key  float64
	}
	var rows []sortableRow
	for in.HasMore() {
		heads, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		row := heads[0]
		var v float64
		key, _ := contextKwargs.Get("by")
		switch key := key.(type) {
		case value.Int:
			v = float64(key)
		case value.Double:
			v = float64(key)
		default:
			expType := op.Signature().ContextKwargs["by"].Type
			return fmt.Errorf("value of context kwarg 'by' is not of type '%s': %s", expType, expType.Validate(key))
		}
		rows = append(rows, sortableRow{data: row, key: v})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		v, _ := staticKwargs.Get("reverse")
		if !v.(value.Bool) {
			return rows[i].key < rows[j].key
		} else {
			return rows[i].key > rows[j].key
		}
	})
	for _, row := range rows {
		if err := out.Append(row.data); err != nil {
			return err
		}
	}
	return nil
}
