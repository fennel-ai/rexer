package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type rollingSum struct {
	name     ftypes.AggName
	Duration uint64
	Bucketizer
	BucketStore
}

func NewSum(name ftypes.AggName, duration uint64) Histogram {
	return rollingSum{
		name:     name,
		Duration: duration,
		Bucketizer: fixedWidthBucketizer{map[ftypes.Window]uint64{
			ftypes.Window_MINUTE: 6,
			ftypes.Window_DAY:    1,
		}, true},
		// retain all keys for 1.5days + duration
		BucketStore: NewTwoLevelStorage(24*3600, duration+24*3600*1.5),
	}
}

func (r rollingSum) Name() ftypes.AggName {
	return r.name
}

func (r rollingSum) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	d, err := extractDuration(kwargs, r.Duration)
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
	if _, ok := a.(value.Int); !ok {
		return nil, fmt.Errorf("expected int but got: %v", a)
	}
	return a.Op("+", b)
}

func (r rollingSum) Zero() value.Value {
	return value.Int(0)
}

func (r rollingSum) Transform(v value.Value) (value.Value, error) {
	if _, ok := v.(value.Int); !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	return v, nil
}

var _ Histogram = rollingSum{}
