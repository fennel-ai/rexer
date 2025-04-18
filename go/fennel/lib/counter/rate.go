package counter

import (
	"fmt"
	"strconv"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/utils/math"
	"fennel/lib/value"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var rateNumGTDen = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "rolling_rate_num_gt_den",
		Help: "Total number of normalized rate counter reductions with numerator > denominator",
	},
	[]string{"aggId"},
)

var zeroRate value.Value = value.NewList(value.Double(0), value.Double(0))

/*
	rollingRate maintains a rate (say actions per click)
	It stores two numbers - num (numerator) and den (denominator)
*/
type rollingRate struct {
	opts aggregate.Options

	aggId ftypes.AggId
}

var _ MergeReduce = rollingRate{}

func NewRate(aggId ftypes.AggId, opts aggregate.Options) rollingRate {
	return rollingRate{
		aggId: aggId,
		opts:  opts,
	}
}

func (r rollingRate) Options() aggregate.Options {
	return r.opts
}

func (r rollingRate) Transform(v value.Value) (value.Value, error) {
	a, b, err := r.extract(v)
	if err != nil {
		return nil, err
	}
	return value.NewList(value.Double(a), value.Double(b)), nil
}

func (r rollingRate) extract(v value.Value) (float64, float64, error) {
	l, ok := v.(value.List)
	if !ok || l.Len() != 2 {
		return 0, 0, fmt.Errorf("expected list of two elements but got: %v", v)
	}
	e, _ := l.At(0)
	first, err := getDouble(e)
	if err != nil {
		return 0, 0, err
	}
	e, _ = l.At(1)
	second, err := getDouble(e)
	if err != nil {
		return 0, 0, err
	}

	if first < 0 || second < 0 {
		return 0, 0, fmt.Errorf("numerator & denominator should be non-negative but found: '%f', '%f' instead", first, second)
	}
	return first, second, nil
}

func (r rollingRate) Reduce(values []value.Value) (value.Value, error) {
	var num, den float64 = 0, 0
	for _, v := range values {
		n, d, err := r.extract(v)
		if err != nil {
			return nil, err
		}
		num += n
		den += d
	}
	if den == 0 {
		return value.Double(0), nil
	}
	var ratio float64
	var err error
	if r.opts.Normalize {
		// TODO(Mohit): Consider making this an error in the future once Lokal's counters data has been evicted
		if num > den {
			// report metrics for this case
			rateNumGTDen.WithLabelValues(strconv.Itoa(int(r.aggId))).Inc()
			// set the ratio as 1.0
			ratio = 1.0
		} else {
			ratio, err = math.Wilson(num, den, true)
			if err != nil {
				return nil, err
			}
		}
	} else {
		ratio = num / den
	}

	return value.Double(ratio), nil
}

func (r rollingRate) Merge(a, b value.Value) (value.Value, error) {
	n1, d1, err := r.extract(a)
	if err != nil {
		return nil, err
	}
	n2, d2, err := r.extract(b)
	if err != nil {
		return nil, err
	}
	return value.NewList(value.Double(n1+n2), value.Double(d1+d2)), nil
}

func (r rollingRate) Zero() value.Value {
	return zeroRate
}
