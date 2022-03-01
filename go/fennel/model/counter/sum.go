package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type RollingCounter struct {
	Duration uint64
}

func (r RollingCounter) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return start(end, r.Duration)
}

func (r RollingCounter) Reduce(values []value.Value) (value.Value, error) {
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

func (r RollingCounter) Merge(a, b value.Value) (value.Value, error) {
	if _, ok := a.(value.Int); !ok {
		return nil, fmt.Errorf("expected int but got: %v", a)
	}
	return a.Op("+", b)
}

func (r RollingCounter) Zero() value.Value {
	return value.Int(0)
}
func (r RollingCounter) Bucketize(groupkey string, v value.Value, timestamp ftypes.Timestamp) ([]Bucket, error) {
	if _, ok := v.(value.Int); !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	return BucketizeMoment(groupkey, timestamp, v, r.Windows()), nil
}

func (r RollingCounter) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

var _ Histogram = RollingCounter{}
