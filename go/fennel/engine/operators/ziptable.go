package operators

import (
	"fmt"
	"strconv"

	"fennel/lib/value"
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
	d := toDict(first)
	zt.first.Append(d)
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
	asdict, ok := first.(value.Dict)
	if !ok {
		return nil, value.Dict{}, fmt.Errorf("expected dict of operands but found: %s", first)
	}
	aslist, err := fromDict(asdict)
	if err != nil {
		return nil, value.Dict{}, err
	}
	second_val, err := zi.second.Next()
	if err != nil {
		return nil, value.Dict{}, err
	}
	second := second_val.(value.Dict)
	if err = Typecheck(zi.sig, aslist, second); err != nil {
		return nil, value.Dict{}, err
	}
	return aslist, second, nil
}

func toDict(elems []value.Value) value.Value {
	m := make(map[string]value.Value, len(elems))
	for i := range elems {
		k := fmt.Sprintf("%d", i)
		m[k] = elems[i]
	}
	return value.NewDict(m)
}

func fromDict(d value.Dict) ([]value.Value, error) {
	ret := make([]value.Value, d.Len())
	for k, v := range d.Iter() {
		n, err := strconv.Atoi(k)
		if err != nil {
			return nil, err
		}
		if n < 0 || n >= len(ret) {
			return nil, fmt.Errorf("unexpected index in dictionary: %d", n)
		}
		ret[n] = v
	}
	return ret, nil
}
