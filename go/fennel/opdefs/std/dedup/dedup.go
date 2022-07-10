package dedup

import (
	"context"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	err := operators.Register(deduper{})
	if err != nil {
		panic(err)
	}
}

type deduper struct{}

func (d deduper) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return deduper{}, nil
}

func (d deduper) Apply(_ context.Context, _ operators.Kwargs, in operators.InputIter, out *value.List) error {
	seen := make(map[string]struct{})
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		row := heads[0]
		by := kwargs.GetUnsafe("by")
		if by == nil {
			by = row
		}
		k := by.String()
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out.Append(row)
	}
	return nil
}

func (d deduper) Signature() *operators.Signature {
	return operators.NewSignature("std", "dedup").
		Param("by", value.Types.Any, false, true, nil)
}

var _ operators.Operator = deduper{}
