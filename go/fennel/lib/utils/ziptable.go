package utils

import (
	"fennel/lib/value"
	"reflect"
)

// ZipTable represents two value tables that can be iterated in a zipped fashion
type ZipTable struct {
	first  *value.Table
	second *value.Table
}

func NewZipTable() ZipTable {
	first := value.NewTable()
	second := value.NewTable()
	return ZipTable{&first, &second}
}

// TODO: this almost certainly has weird race conditions if run in paralle. Fix
func (zt ZipTable) Append(first, second value.Dict) error {
	if err := zt.first.Append(first); err != nil {
		return err
	}
	if err := zt.second.Append(second); err != nil {
		// the first append had gone through so remove it too
		zt.first.Pop()
		return err
	}
	return nil
}

func (zt ZipTable) Schema() (map[string]reflect.Type, map[string]reflect.Type) {
	return zt.first.Schema(), zt.second.Schema()
}

func (zt ZipTable) Len() int {
	return zt.first.Len()
}

func (zt ZipTable) Iter() ZipIter {
	first := zt.first.Iter()
	second := zt.second.Iter()
	return ZipIter{
		first:  &first,
		second: &second,
	}
}

type ZipIter struct {
	first  *value.Iter
	second *value.Iter
}

func (zi *ZipIter) HasMore() bool {
	return zi.first.HasMore() && zi.second.HasMore()
}

func (zi *ZipIter) Next() (value.Dict, value.Dict, error) {
	first, err := zi.first.Next()
	if err != nil {
		return value.Dict{}, value.Dict{}, err
	}
	second, err := zi.second.Next()
	if err != nil {
		return value.Dict{}, value.Dict{}, err
	}
	return first, second, nil
}
