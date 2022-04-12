package std

import (
	"context"
	"fmt"
	"sync"

	"fennel/engine/operators"
	"fennel/lib/value"
)

type FlattenOperator struct{}

var _ operators.Operator = FlattenOperator{}

func (op FlattenOperator) New(
	args value.Dict, bootargs map[string]interface{}, cache *sync.Map,
) (operators.Operator, error) {
	return FlattenOperator{}, nil
}

func (op FlattenOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "flatten").
		Input([]value.Type{value.Types.Any}).
		ParamWithHelp("depth", value.Types.Int, true, true, value.Int(0),
			"StaticKwargs: Number of levels to flatten. At '0' flattens to a 1-dimensional list")
}

func (op FlattenOperator) Apply(_ context.Context, staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	depth := int64(staticKwargs.GetUnsafe("depth").(value.Int))
	if depth < 0 {
		return fmt.Errorf("static kwarg 'depth' cannot be negative")
	}
	// modify depth to work with op.flatten()
	if depth == 0 {
		depth = -1
	}
	for in.HasMore() {
		heads, _, err := in.Next()
		if err != nil {
			return err
		}
		l := heads[0].(value.List)
		vals := op.flatten(l, depth)
		out.Append(vals.Values()...)
	}
	return nil
}

// flatten recursively flattens the given list upto specified depth
// if depth is negative, flattens until list is 1D
func (op FlattenOperator) flatten(l value.List, depth int64) value.List {
	if depth == 0 {
		return l
	}
	res := value.NewList()
	for i := 0; i < l.Len(); i++ {
		v, _ := l.At(i)
		lv, ok := v.(value.List)
		if ok {
			res.Append(op.flatten(lv, depth-1).Values()...)
		} else {
			res.Append(v)
		}
	}
	return res
}
