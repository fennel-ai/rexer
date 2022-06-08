package operators

import (
	"errors"
	"fennel/lib/value"
	"fmt"
)

// ZipTable represents a list of values (inputs) and list of dicts (contextual kwargs)
type ZipTable struct {
	first  []value.Value
	second []value.Dict
	sig    *Signature
}

func NewZipTable(op Operator) ZipTable {
	sig := op.Signature()
	return ZipTable{first: nil, second: nil, sig: sig}
}

func (zt *ZipTable) Append(first []value.Value, second value.Dict) error {
	d := value.NewList(first...)
	zt.first = append(zt.first, d)
	zt.second = append(zt.second, second)
	return nil
}

func (zt *ZipTable) Grow(n int) {
	if cap(zt.first) >= len(zt.first)+n {
		return
	}
	first := make([]value.Value, len(zt.first), len(zt.first)+n)
	copy(first, zt.first)
	zt.first = first

	second := make([]value.Dict, len(zt.first), len(zt.first)+n)
	copy(second, zt.second)
	zt.second = second
}

func (zt *ZipTable) Len() int {
	return len(zt.first)
}

func (zt *ZipTable) Iter() ZipIter {
	return ZipIter{idx: 0, zt: zt}
}

type ZipIter struct {
	idx int
	zt  *ZipTable
}

func (zi *ZipIter) HasMore() bool {
	return zi.idx < len(zi.zt.first)
}

func (zi *ZipIter) Next() ([]value.Value, value.Dict, error) {
	idx := zi.idx
	zi.idx += 1
	if idx >= len(zi.zt.first) {
		return nil, value.Dict{}, errors.New("no more elements in zip iter")
	}
	first := zi.zt.first[idx]
	second := zi.zt.second[idx]
	aslist, ok := first.(value.List)
	if !ok {
		return nil, value.Dict{}, fmt.Errorf("expected list of operands but found: %s", first)
	}
	elems := aslist.Values()
	if err := Typecheck(zi.zt.sig, elems, second); err != nil {
		return nil, value.Dict{}, err
	}
	return elems, second, nil
}
