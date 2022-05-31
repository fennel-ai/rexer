package operators

import (
	"fennel/lib/value"
	"fmt"
)

// ZipTable represents a list of values (inputs) and list of dicts (contextual kwargs)
type ZipTable struct {
	first  value.List
	second value.List
	sig    *Signature
}

func NewZipTable(op Operator) ZipTable {
	first := value.NewList()
	second := value.NewList()
	sig := op.Signature()
	return ZipTable{first, second, sig}
}

// TODO: this almost certainly has weird race conditions if run in paralle. Fix
func (zt *ZipTable) Append(first []value.Value, second value.Dict) error {
	d := value.NewList(first...)
	zt.first.Append(d)
	zt.second.Append(second)
	return nil
}

func (zt *ZipTable) Grow(n int) {
	if cap(zt.first.Values()) >= zt.first.Len()+n {
		return
	}
	zt.first.Grow(n)
	zt.second.Grow(n)
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
		sig:    zt.sig,
	}
}

type ZipIter struct {
	first  *value.Iter
	second *value.Iter
	sig    *Signature
}

func (zi *ZipIter) HasMore() bool {
	return zi.first.HasMore() && zi.second.HasMore()
}

func (zi *ZipIter) Next() ([]value.Value, value.Dict, error) {
	first, err := zi.first.Next()
	if err != nil {
		return nil, value.Dict{}, err
	}
	aslist, ok := first.(value.List)
	if !ok {
		return nil, value.Dict{}, fmt.Errorf("expected list of operands but found: %s", first)
	}
	elems := make([]value.Value, aslist.Len())
	for i := 0; i < aslist.Len(); i++ {
		elems[i], _ = aslist.At(i)
	}
	second_val, err := zi.second.Next()
	if err != nil {
		return nil, value.Dict{}, err
	}
	second := second_val.(value.Dict)
	if err = Typecheck(zi.sig, elems, second); err != nil {
		return nil, value.Dict{}, err
	}
	return elems, second, nil
}
