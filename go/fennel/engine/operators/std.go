package operators

import (
	"fennel/lib/value"
)

func init() {
	registry = make(map[string]map[string]Operator)
	ops := []Operator{FilterOperator{}, TakeOperator{}, AddColumnOperator{}}
	for _, op := range ops {
		if err := Register(op); err != nil {
			panic(err)
		}
	}
}

type FilterOperator struct{}

func (f FilterOperator) Init(_ value.Dict, _ map[string]interface{}) error {
	return nil
}

func (f FilterOperator) Signature() *Signature {
	return NewSignature(f, "std", "filter").
		Param("where", value.Types.Bool, false)
}

func (f FilterOperator) Apply(_ value.Dict, in InputIter, out *value.Table) error {
	for in.HasMore() {
		row, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		where := contextKwargs["where"].(value.Bool)
		if where {
			out.Append(row)
		}
	}
	return nil
}

type TakeOperator struct{}

func (f TakeOperator) Init(_ value.Dict, _ map[string]interface{}) error {
	return nil
}

func (f TakeOperator) Signature() *Signature {
	return NewSignature(f, "std", "take").
		Param("limit", value.Types.Int, true)
}

func (f TakeOperator) Apply(staticKwargs value.Dict, in InputIter, out *value.Table) error {
	limit := staticKwargs["limit"].(value.Int)
	taken := 0
	for in.HasMore() && taken < int(limit) {
		row, _, err := in.Next()
		if err != nil {
			return err
		}
		out.Append(row)
		taken += 1
	}
	return nil
}

type AddColumnOperator struct{}

var _ Operator = AddColumnOperator{}

func (op AddColumnOperator) Init(_ value.Dict, _ map[string]interface{}) error {
	return nil
}

func (op AddColumnOperator) Signature() *Signature {
	return NewSignature(op, "std", "addColumn").
		Param("name", value.Types.String, true).
		Param("value", value.Types.Any, false)
}

func (op AddColumnOperator) Apply(staticKwargs value.Dict, in InputIter, out *value.Table) error {
	name := string(staticKwargs["name"].(value.String))
	for in.HasMore() {
		row, contextKwargs, err := in.Next()
		if err != nil {
			return err
		}
		row[name] = contextKwargs["value"]
		out.Append(row)
	}
	return nil
}
