package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type Stream struct {
	Duration uint64
}

func (s Stream) extract(v value.Value) (value.List, error) {
	l, ok := v.(value.List)
	if !ok {
		return value.List{}, fmt.Errorf("value expected to be list but instead found: %v", v)
	}
	return l, nil
}

func (s Stream) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return end - ftypes.Timestamp(s.Duration)
}

// Reduce just appends all the lists to an empty list
func (s Stream) Reduce(values []value.Value) (value.Value, error) {
	z := s.Zero().(value.List)
	for i := range values {
		l, err := s.extract(values[i])
		if err != nil {
			return nil, err
		}
		z = append(z, l...)
	}
	return z, nil
}

func (s Stream) Merge(a, b value.Value) (value.Value, error) {
	la, err := s.extract(a)
	if err != nil {
		return nil, err
	}
	lb, err := s.extract(b)
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, 0, len(la)+len(lb))
	ret = append(ret, la...)
	ret = append(ret, lb...)
	return value.List(ret), nil
}

func (s Stream) Zero() value.Value {
	return value.List{}
}

func (s Stream) Bucketize(actions value.Table) ([]Bucket, error) {
	schema := actions.Schema()
	_, ok := schema["key"]
	if !ok {
		return nil, fmt.Errorf("expected field 'key' not present")
	}
	type_, ok := schema["timestamp"]
	if !ok || type_ != value.Types.Int {
		return nil, fmt.Errorf("expected field 'timestamp' not present")
	}
	if _, ok = schema["element"]; !ok {
		return nil, fmt.Errorf("expected field 'element' not present")
	}
	buckets := make([]Bucket, 0, actions.Len())
	for _, row := range actions.Pull() {
		ts := row["timestamp"].(value.Int)
		key := row["key"]
		element := row["element"]
		buckets = append(buckets, BucketizeMoment(key.String(), ftypes.Timestamp(ts), value.List{element}, s.Windows())...)
	}
	return buckets, nil
}

func (s Stream) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

var _ Histogram = Stream{}
