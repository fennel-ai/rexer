package std

import (
	"context"
	"fmt"
	"sort"

	"fennel/engine/operators"
	"fennel/lib/value"
)

type SortOperator struct{}

var _ operators.Operator = SortOperator{}

func (op SortOperator) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return SortOperator{}, nil
}

func (op SortOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "sort").
		ParamWithHelp("by", value.Types.Any, false, true, nil, "The value to be sorted on").
		ParamWithHelp("reverse", value.Types.Bool, true, true, value.Bool(false), "StaticKwargs: Set true to get descending sort order")
}

func (op SortOperator) Apply(_ context.Context, staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	type sortableRow struct {
		key  float64
		data value.Value
	}
	var rows []sortableRow
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		var key value.Value
		by, _ := kwargs.Get("by")
		if by == nil {
			key = heads[0]
		} else {
			key = by
		}
		var v float64
		switch key := key.(type) {
		case value.Int:
			v = float64(key)
		case value.Double:
			v = float64(key)
		default:
			return fmt.Errorf("sort key should be a number. Got [%s]", key.String())
		}
		rows = append(rows, sortableRow{data: heads[0], key: v})
	}
	rk, _ := staticKwargs.Get("reverse")
	reverse := rk.(value.Bool)
	sort.SliceStable(rows, func(i, j int) bool {
		if !reverse {
			return rows[i].key < rows[j].key
		} else {
			return rows[i].key > rows[j].key
		}
	})
	out.Grow(len(rows))
	for _, row := range rows {
		out.Append(row.data)
	}
	return nil
}
