package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

var zeroSum value.Value = value.Int(0)

type rollingSum struct {
	Durations []uint64
	BucketStore
}

var _ Histogram = rollingSum{}

func NewSum(durations []uint64) Histogram {
	maxDuration := getMaxDuration(durations)
	return rollingSum{
		Durations: durations,
		// retain all keys for 1.1days(95040) + duration
		BucketStore: NewTwoLevelStorage(24*3600, maxDuration+95040),
	}
}

func (r rollingSum) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	d, err := extractDuration(kwargs, r.Durations)
	if err != nil {
		return 0, err
	}
	return start(end, d), nil
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
