package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type rollingSum struct {
	Duration uint64
	Bucketizer
	BucketStore
}

func NewSum(name ftypes.AggName, duration uint64) Histogram {
	return rollingSum{
		Duration: duration,
		Bucketizer: fixedWidthBucketizer{map[ftypes.Window]uint64{
			ftypes.Window_MINUTE: 1, ftypes.Window_HOUR: 1, ftypes.Window_DAY: 1,
		}},
		BucketStore: FlatRedisStorage{name: name},
	}
}

func (r rollingSum) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return start(end, r.Duration)
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
