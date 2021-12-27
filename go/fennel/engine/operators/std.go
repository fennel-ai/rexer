package operators

import (
	"fennel/value"
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
		Param("where", reflect.TypeOf(value.Bool(true)))
}

func (f FilterOperator) Apply(kwargs value.Dict, in value.Table, out *value.Table) error {
	for _, row := range in.Pull() {
		where := kwargs["where"].(value.Bool)
		if where {
			out.Append(row)
		}
	}
	return nil
}

type TakeOperator struct{}

func (f TakeOperator) Signature() *Signature {
	return NewSignature().
		Param("limit", reflect.TypeOf(value.Int(1)))
}

func (f TakeOperator) Apply(kwargs value.Dict, in value.Table, out *value.Table) error {
	limit := kwargs["limit"].(value.Int)
	for i, row := range in.Pull() {
		if i >= int(limit) {
			break
		}
		out.Append(row)
	}
	return nil
}
