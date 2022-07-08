package counter

import (
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/value"
)

var zeroTsSum value.Value = value.Int(0)

type timeseriesSum struct {
	opts aggregate.Options
}

var _ MergeReduce = timeseriesSum{}

func NewTimeseriesSum(opts aggregate.Options) MergeReduce {
	return timeseriesSum{opts}
}

func (r timeseriesSum) Options() aggregate.Options { return r.opts }

func (r timeseriesSum) Reduce(values []value.Value) (value.Value, error) {
	// we have to take the last Limit values only and if there are fewer than that
	// available we pad a few entries with zeros.
	limit := int(r.opts.Limit)
	last := len(values) - 1
	ret := make([]value.Value, r.opts.Limit)
	var i int
	for i = 0; i < limit && i < len(values); i++ {
		ret[limit-1-i] = values[last-i]
	}
	for ; i < limit; i++ {
		ret[limit-1-i] = value.Int(0)
	}
	return value.NewList(ret...), nil
}

func (r timeseriesSum) Merge(a, b value.Value) (value.Value, error) {
	if _, ok := a.(value.Int); !ok {
		return nil, fmt.Errorf("expected int but got: %v", a)
	}
	return a.Op("+", b)
}

func (r timeseriesSum) Zero() value.Value {
	return zeroTsSum
}

func (r timeseriesSum) Transform(v value.Value) (value.Value, error) {
	if _, ok := v.(value.Int); !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	return v, nil
}
