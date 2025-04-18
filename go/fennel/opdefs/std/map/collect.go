package _map

import (
	"context"
	"errors"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	err := operators.Register(collect{})
	if err != nil {
		panic(err)
	}
}

type collect struct {
}

func (m collect) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return collect{}, nil
}

func (m collect) Apply(_ context.Context, _ operators.Kwargs, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		row := heads[0].(value.Dict)

		if err != nil {
			return err
		}

		key_list, _ := kwargs.Get("fields")
		keys := key_list.(value.List)
		keyiter := keys.Iter()
		values := make([]value.Value, keys.Len())
		index := 0
		for keyiter.HasMore() {
			key_val, _ := keyiter.Next()
			key_str, ok := key_val.(value.String)
			if !ok {
				return errors.New("keys in map lookup must be evaluate to a string")
			}
			values[index], _ = row.Get(string(key_str))
			index++
		}

		out.Append(value.NewList(values...))
	}
	return nil
}

func (m collect) Signature() *operators.Signature {
	return operators.NewSignature("std", "collect").
		Input([]value.Type{value.Types.Dict}).
		ParamWithHelp("fields", value.Types.List, false, false, value.Nil, "ContextKwarg: List of keys to lookup in the map")
}

var _ operators.Operator = collect{}
