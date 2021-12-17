package operators

import (
	"engine/runtime"
	"reflect"
)

func init() {
	registry["std"] = map[string]Operator{
		"filter": FilterOperator{},
		"take":   TakeOperator{},
	}
}

type FilterOperator struct{}

func (f FilterOperator) Signature() *Signature {
	return NewSignature().
		Param("where", reflect.TypeOf(runtime.Bool(true)))
}

func (f FilterOperator) Apply(kwargs runtime.Dict, in runtime.Table, out *runtime.Table) error {
	for _, row := range in.Pull() {
		where := kwargs["where"].(runtime.Bool)
		if where {
			out.Append(row)
		}
	}
	return nil
}

type TakeOperator struct{}

func (f TakeOperator) Signature() *Signature {
	return NewSignature().
		Param("limit", reflect.TypeOf(runtime.Int(1)))
}

func (f TakeOperator) Apply(kwargs runtime.Dict, in runtime.Table, out *runtime.Table) error {
	limit := kwargs["limit"].(runtime.Int)
	for i, row := range in.Pull() {
		if i >= int(limit) {
			break
		}
		out.Append(row)
	}
	return nil
}
