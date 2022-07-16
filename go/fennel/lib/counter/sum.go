package counter

import (
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/value"
)

var zeroSum value.Value = value.Int(0)

type rollingSum struct {
	opts aggregate.Options
}

var _ MergeReduce = rollingSum{}

func NewSum(opts aggregate.Options) rollingSum {
	return rollingSum{opts}
}

func (r rollingSum) Options() aggregate.Options {
	return r.opts
}

func (r rollingSum) Reduce(values []value.Value) (value.Value, error) {
	var total value.Value = value.Int(0)
	var err error
	for i := range values {
		total, err = total.Op("+", values[i])
		if err != nil {
			return nil, err
		}
	}
	return total, nil
}

func (r rollingSum) Merge(a, b value.Value) (value.Value, error) {
	if err := value.Types.Number.Validate(a); err != nil {
		return nil, fmt.Errorf("value [%s] is not a number", a.String())
	}
	if err := value.Types.Number.Validate(b); err != nil {
		return nil, fmt.Errorf("value [%s] is not a number", b.String())
	}

	return a.Op("+", b)
}

func (r rollingSum) Zero() value.Value {
	return zeroSum
}

func (r rollingSum) Transform(v value.Value) (value.Value, error) {
	if err := value.Types.Number.Validate(v); err != nil {
		return nil, fmt.Errorf("value [%s] is not a number", v.String())
	}
	return v, nil
}
