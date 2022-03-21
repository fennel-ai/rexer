package operators

import (
	"fennel/lib/value"
)

// ZipTable represents a list of values (input) and list of dicts (contextual kwargs)
type ZipTable struct {
	first  value.List
	second value.List
	op     Operator
}

func NewZipTable(op Operator) ZipTable {
	first := value.NewList()
	second := value.NewList()
	return ZipTable{first, second, op}
}

// TODO: this almost certainly has weird race conditions if run in paralle. Fix
func (zt *ZipTable) Append(first value.Value, second value.Dict) error {
	if err := zt.first.Append(first); err != nil {
		return err
	}
	zt.second.Append(second)
	return nil
}

func (zt *ZipTable) Len() int {
	return zt.first.Len()
}

func (zt *ZipTable) Iter() ZipIter {
	first := zt.first.Iter()
	second := zt.second.Iter()
	return ZipIter{
		first:  &first,
		second: &second,
		op:     zt.op,
	}
}

type ZipIter struct {
	first  *value.Iter
	second *value.Iter
	op     Operator
}

func (zi *ZipIter) HasMore() bool {
	return zi.first.HasMore() && zi.second.HasMore()
}

func (zi *ZipIter) Next() (value.Value, value.Dict, error) {
	first, err := zi.first.Next()
	if err != nil {
		return value.Dict{}, value.Dict{}, err
	}
	second_val, err := zi.second.Next()
	if err != nil {
		return value.Dict{}, value.Dict{}, err
	}
	second := second_val.(value.Dict)
	if err = Typecheck(zi.op, first, second); err != nil {
		return nil, value.Dict{}, err
	}
	return first, second, nil
}
