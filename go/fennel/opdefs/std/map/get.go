package _map

import (
	"context"
	"errors"
	"sync"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(get{})
}

type get struct {
}

func (m get) New(
	args value.Dict, bootargs map[string]interface{}, cache *sync.Map,
) (operators.Operator, error) {
	return get{}, nil
}

func (m get) Apply(_ context.Context, staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
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

func (m get) Signature() *operators.Signature {
	return operators.NewSignature("std", "map_lookup").
		Input([]value.Type{value.Types.Dict}).
		ParamWithHelp("fields", value.Types.List, false, false, value.Nil, "ContextKwarg: List of keys to lookup in the map")
}

var _ operators.Operator = get{}
