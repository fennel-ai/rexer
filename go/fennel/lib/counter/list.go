package counter

import (
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/value"
)

var zeroList value.Value = value.NewList()

type list struct {
	opts aggregate.Options
}

var _ MergeReduce = list{}

func (s list) Transform(v value.Value) (value.Value, error) {
	return value.NewList(v), nil
}

func NewList(opts aggregate.Options) list {
	return list{opts}
}

func (s list) Options() aggregate.Options {
	return s.opts
}

func (s list) extract(v value.Value) (value.List, error) {
	l, ok := v.(value.List)
	if !ok {
		return value.List{}, fmt.Errorf("value expected to be list but instead found: %v", v)
	}
	return l, nil
}

// Reduce just appends all the lists to an empty list
func (s list) Reduce(values []value.Value) (value.Value, error) {
	m := make(map[string]value.Value)
	for i := range values {
		l, err := s.extract(values[i])
		if err != nil {
			return nil, err
		}
		for j := 0; j < l.Len(); j++ {
			val, _ := l.At(j)
			m[val.String()] = val
		}
	}
	z := s.Zero().Clone().(value.List)
	for _, v := range m {
		z.Append(v)
	}
	return z, nil
}

func (s list) Merge(a, b value.Value) (value.Value, error) {
	la, err := s.extract(a)
	if err != nil {
		return nil, err
	}
	lb, err := s.extract(b)
	if err != nil {
		return nil, err
	}
	m := make(map[string]value.Value, la.Len())
	for j := 0; j < la.Len(); j++ {
		val, _ := la.At(j)
		m[val.String()] = val
	}
	for j := 0; j < lb.Len(); j++ {
		val, _ := lb.At(j)
		m[val.String()] = val
	}
	ret := value.NewList()
	ret.Grow(len(m))
	for _, v := range m {
		ret.Append(v)
	}
	return ret, nil
}

func (s list) Zero() value.Value {
	return zeroList
}
