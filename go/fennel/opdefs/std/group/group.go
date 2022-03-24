package group

import (
	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(grouper{})
}

type grouper struct {
}

func (g grouper) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return grouper{}, nil
}

func (g grouper) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	groups := make([]string, 0)
	bys := make([]value.Value, 0)
	elements := make(map[string][]value.Value)
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		row := heads[0]
		by := kwargs.GetUnsafe("by")
		key := by.String()
		if _, ok := elements[key]; !ok {
			groups = append(groups, key)
			bys = append(bys, by)
			elements[key] = make([]value.Value, 0)
		}
		elements[key] = append(elements[key], row)
	}
	for i, g := range groups {
		d := value.NewDict(map[string]value.Value{
			"group":    bys[i],
			"elements": value.NewList(elements[g]...),
		})
		if err := out.Append(d); err != nil {
			return err
		}
	}
	return nil
}

func (g grouper) Signature() *operators.Signature {
	return operators.NewSignature("std", "group").
		Param("by", value.Types.Any, false, false, nil)
}

var _ operators.Operator = grouper{}
