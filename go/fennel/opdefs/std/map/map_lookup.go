package _map

import (
	"context"
	"errors"
	"sync"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(map_lookup{})
}

type map_lookup struct {
}

func (m map_lookup) New(
	args value.Dict, bootargs map[string]interface{}, cache *sync.Map,
) (operators.Operator, error) {
	return map_lookup{}, nil
}

func (m map_lookup) Apply(_ context.Context, staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		row := heads[0].(value.Dict)

		if err != nil {
			return err
		}

		key_list, _ := kwargs.Get("keys")
		keys := key_list.(value.List)
		keyiter := keys.Iter()
		values := make([]value.Value, keys.Len())
		index := 0
		for keyiter.HasMore() {
			key_val, _ := keyiter.Next()
			key_str, ok := key_val.(value.String)
			if !ok {
				return errors.New("keys in map lookup must be evaluate to strings")
			}
			values[index] = get(row, string(key_str))
			index++
		}

		out.Append(value.NewList(values...))
	}
	return nil
}

func (m map_lookup) Signature() *operators.Signature {
	return operators.NewSignature("std", "map_lookup").
		Input([]value.Type{value.Types.Dict}).
		ParamWithHelp("keys", value.Types.List, false, false, value.Nil, "ContextKwarg: List of keys to lookup in the map")
}

var _ operators.Operator = mapper{}

func get(d value.Dict, k string) value.Value {
	ret, _ := d.Get(k)
	return ret
}
