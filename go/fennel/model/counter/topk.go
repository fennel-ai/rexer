package counter

import (
	"fmt"
	"sort"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

const numK = 100

type topK struct {
	Durations []uint64
	Bucketizer
	BucketStore
}

func NewTopK(name ftypes.AggName, durations []uint64) Histogram {
	maxDuration := getMaxDuration(durations)
	return topK{
		Durations: durations,
		Bucketizer: fixedWidthBucketizer{map[ftypes.Window]uint64{
			ftypes.Window_MINUTE: 6,
			ftypes.Window_DAY:    1,
		}, true},
		// retain all keys for 1.5days + duration
		BucketStore: NewTwoLevelStorage(24*3600, maxDuration+24*3600*1.5),
	}
}

func (t topK) Transform(v value.Value) (value.Value, error) {
	elem, ok := v.(value.Dict)
	if !ok {
		return nil, fmt.Errorf("expected value to be a dict but got: '%s' instead", v)
	}
	key, ok := elem.Get("key")
	if !ok {
		return nil, fmt.Errorf("field 'key' not found in dict")
	}
	keystr, ok := key.(value.String)
	if !ok {
		return nil, fmt.Errorf("expected field 'key' to be a string but got '%s' instead", key)
	}
	sval, ok := elem.Get("score")
	if !ok {
		return nil, fmt.Errorf("field 'score' not found in dict")
	}
	var score value.Double
	switch s := sval.(type) {
	case value.Int:
		score = value.Double(s)
	case value.Double:
		score = s
	default:
		return nil, fmt.Errorf("expected field 'score' of dict to be an int or float but got: '%s' instead", s)
	}
	return value.NewDict(map[string]value.Value{string(keystr): score}), nil
}

func (t topK) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	d, err := extractDuration(kwargs, t.Durations)
	if err != nil {
		return 0, err
	}
	return start(end, d), nil
}

func (t topK) Reduce(values []value.Value) (value.Value, error) {
	all := make([]value.Dict, len(values))
	var err error
	for i, v := range values {
		all[i], err = t.extract(v)
		if err != nil {
			return nil, err
		}
	}
	d, err := t.merge(all)
	if err != nil {
		return nil, err
	}
	// transform to list of lists (first element stores key, second stores the value) and sort
	l := make([]value.Value, 0, len(values))
	for k, v := range d.Iter() {
		l = append(l, value.NewList(value.String(k), v))
	}
	sort.SliceStable(l, func(i, j int) bool {
		vI, _ := l[i].(value.List).At(1)
		vJ, _ := l[j].(value.List).At(1)
		less, _ := vI.Op(">", vJ)
		return bool(less.(value.Bool))
	})
	if len(l) > numK {
		return value.NewList(l[:numK]...), nil
	} else {
		return value.NewList(l...), nil
	}
}

func (t topK) Merge(a, b value.Value) (value.Value, error) {
	da, err := t.extract(a)
	if err != nil {
		return nil, err
	}
	db, err := t.extract(b)
	if err != nil {
		return nil, err
	}
	return t.merge([]value.Dict{da, db})
}

func (t topK) Zero() value.Value {
	return value.NewDict(map[string]value.Value{})
}

func (t topK) extract(v value.Value) (value.Dict, error) {
	d, ok := v.(value.Dict)
	if !ok {
		return value.NewDict(map[string]value.Value{}), fmt.Errorf("expected dict but got: %v", v)
	}
	// must typecheck for sorting in Reduce to not panic
	for _, v := range d.Iter() {
		switch v.(type) {
		case value.Int, value.Double:
		default:
			return value.Dict{}, fmt.Errorf("expected value in dict to be int/float but found: '%v'", v)
		}
	}
	return d, nil
}

func (t topK) merge(ds []value.Dict) (value.Dict, error) {
	ret := value.NewDict(map[string]value.Value{})
	for _, d := range ds {
		for k, v := range d.Iter() {
			vOld, ok := ret.Get(k)
			if !ok {
				ret.Set(k, v)
			} else {
				vNew, err := v.Op("+", vOld)
				if err != nil {
					return value.Dict{}, nil
				}
				ret.Set(k, vNew)
			}
		}
	}
	return ret, nil
}

var _ Histogram = topK{}
