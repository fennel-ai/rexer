package std

import (
	"sort"

	"fennel/engine/operators"
	"fennel/lib/value"
)

type sortOp struct{}

var _ operators.Operator = sortOp{}

type sortableRows struct {
	data value.List
	keys []value.Number
	desc value.Bool
}

func (s sortableRows) Len() int {
	return len(s.data)
}

func (s sortableRows) Less(i, j int) bool {
	if s.desc {
		return s.keys[j].LessThan(s.keys[i])
	} else {
		return s.keys[i].LessThan(s.keys[j])
	}
}

func (s sortableRows) Swap(i, j int) {
	s.data[i], s.data[j] = s.data[j], s.data[i]
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
}

var _ sort.Interface = sortableRows{}

func (op sortOp) Init(_ value.Dict, _ map[string]interface{}) error {
	return nil
}

func (op sortOp) Signature() *operators.Signature {
	return operators.NewSignature("std", "sort", true).
		Input(value.Types.List).
		Param("by", value.Types.Number, false, false, value.Nil).
		Param("desc", value.Types.Bool, true, true, value.Bool(false))
}

func (op sortOp) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	rows := sortableRows{}
	for in.HasMore() {
		row, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		key := contextKwargs["by"].(value.Number)
		rows.data = append(rows.data, row)
		rows.keys = append(rows.keys, key)
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
