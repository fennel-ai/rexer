package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

const numK = 100

type topK struct {
	name     ftypes.AggName
	Duration uint64
	Bucketizer
	BucketStore
}

func NewTopK(name ftypes.AggName, duration uint64) Histogram {
	return topK{
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

func (t topK) Name() ftypes.AggName {
	return t.name
}

func (t topK) Transform(v value.Value) (value.Value, error) {
	elem, ok := v.(value.Dict)
	if !ok {
		return nil, fmt.Errorf("expected value to be a dict but got: '%s' instead", v)
	}
	data, ok := elem.Get("data")
	if !ok {
		return nil, fmt.Errorf("key 'data' not found in dict")
	}
	sval, ok := elem.Get("score")
	if !ok {
		return nil, fmt.Errorf("key 'score' not found in dict")
	}
	var score value.Double
	switch s := sval.(type) {
	case value.Int:
		score = value.Double(s)
	case value.Double:
		score = s
	default:
		return nil, fmt.Errorf("expected key 'score' of dict to be an int or float but got: '%s' instead", s)
	}
	return value.NewList(value.NewDict(map[string]value.Value{"data": data, "score": score})), nil
}

func (t topK) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	d, err := extractDuration(kwargs, t.Duration)
	if err != nil {
		return 0, err
	}
	return start(end, d), nil
}

func (t topK) Reduce(values []value.Value) (value.Value, error) {
	res := make([]value.Value, 0)
	for _, v := range values {
		v, err := t.extract(v)
		if err != nil {
			return nil, err
		}
		res = t.merge(res, v)
	}
	ret := make([]value.Value, len(res))
	for i := 0; i < len(res); i++ {
		v := res[i]
		ret[i], _ = v.(value.Dict)
	}
	return value.NewList(ret...), nil
}

func (t topK) Merge(a, b value.Value) (value.Value, error) {
	la, err := t.extract(a)
	if err != nil {
		return nil, err
	}
	lb, err := t.extract(b)
	if err != nil {
		return nil, err
	}
	lc := t.merge(la, lb)
	return value.NewList(lc...), nil
}

func (t topK) Zero() value.Value {
	return value.List{}
}

func (t topK) extract(v value.Value) ([]value.Value, error) {
	l, ok := v.(value.List)
	if !ok {
		return nil, fmt.Errorf("expected list but got: %v", v)
	}
	var ret []value.Value
	for i := 0; i < l.Len(); i++ {
		elem, _ := l.At(i)
		e, ok := elem.(value.Dict)
		if !ok {
			return nil, fmt.Errorf("expected element of list to be a dict but got: %v", v)
		}
		_, ok = e.Get("data")
		if !ok {
			return nil, fmt.Errorf("key 'data' not found in dict")
		}
		score, ok := e.Get("score")
		if !ok {
			return nil, fmt.Errorf("key 'score' not found in dict")
		}
		_, ok = score.(value.Double)
		if !ok {
			return nil, fmt.Errorf("key 'score' of dict should be a float but got: '%v' instead", score)
		}
		ret = append(ret, e)
	}
	return ret, nil
}

func (t topK) merge(a, b []value.Value) []value.Value {
	n := len(a) + len(b)
	if n > numK {
		n = numK
	}
	c := make([]value.Value, n)
	i, j, k := 0, 0, 0
	for {
		if k == len(c) {
			return c
		}
		if i == len(a) {
			for {
				if j == len(b) || k == len(c) {
					return c
				}
				c[k] = b[j]
				k++
				j++
			}
		}
		if j == len(b) {
			for {
				if i == len(a) || k == len(c) {
					return c
				}
				c[k] = a[i]
				k++
				i++
			}
		}
		if t.less(a[i], b[j]) {
			c[k] = b[j]
			k++
			j++
		} else {
			c[k] = a[i]
			k++
			i++
		}
	}
}

func (t topK) less(a, b value.Value) bool {
	f, _ := a.(value.Dict).Get("score")
	fa := f.(value.Double)
	f, _ = b.(value.Dict).Get("score")
	//fb := b.(value.Dict)["score"].(value.Double)
	fb := f.(value.Double)
	return float64(fa) < float64(fb)
}

var _ Histogram = topK{}
