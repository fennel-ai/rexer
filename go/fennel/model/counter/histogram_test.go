package counter

import (
	"testing"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {
	durations := []uint32{100, 200}
	testHistogramStart(t, Histogram{MergeReduce: NewAverage(aggregate.Options{Durations: durations})}, durations)
	testHistogramStart(t, Histogram{MergeReduce: NewList(aggregate.Options{Durations: durations})}, durations)
	testHistogramStart(t, Histogram{MergeReduce: NewMax(aggregate.Options{Durations: durations})}, durations)
	testHistogramStart(t, Histogram{MergeReduce: NewMin(aggregate.Options{Durations: durations})}, durations)
	testHistogramStart(t, Histogram{MergeReduce: NewRate(1, aggregate.Options{Durations: durations})}, durations)
	testHistogramStart(t, Histogram{MergeReduce: NewStdDev(aggregate.Options{Durations: durations})}, durations)
	testHistogramStart(t, Histogram{MergeReduce: NewSum(aggregate.Options{Durations: durations})}, durations)
	testHistogramStart(t, Histogram{MergeReduce: NewTopK(aggregate.Options{Durations: durations})}, durations)
}

func TestMergeReduceZeroNotMutated(t *testing.T) {
	testHistogramZeroNotMutated(t, Histogram{MergeReduce: NewAverage(aggregate.Options{})}, value.NewList(value.Int(2), value.Int(3)), value.NewList(value.Int(0), value.Int(0)))
	testHistogramZeroNotMutated(t, Histogram{MergeReduce: NewList(aggregate.Options{})}, value.NewList(value.Int(1), value.Int(2)), value.NewList())
	testHistogramZeroNotMutated(t, Histogram{MergeReduce: NewMax(aggregate.Options{})}, value.NewList(value.Double(1.0), value.Bool(true)), value.NewList(value.Double(0), value.Bool(true)))
	testHistogramZeroNotMutated(t, Histogram{MergeReduce: NewMin(aggregate.Options{})}, value.NewList(value.Double(-1.0), value.Bool(true)), value.NewList(value.Double(0), value.Bool(true)))
	testHistogramZeroNotMutated(t, Histogram{MergeReduce: NewRate(1, aggregate.Options{})}, value.NewList(value.Double(1.0), value.Double(2.0)), value.NewList(value.Double(0), value.Double(0)))
	testHistogramZeroNotMutated(t, Histogram{MergeReduce: NewStdDev(aggregate.Options{})}, value.NewList(value.Double(1.0), value.Double(2.0), value.Int(1)), value.NewList(value.Double(0), value.Double(0), value.Int(0)))
	testHistogramZeroNotMutated(t, Histogram{MergeReduce: NewSum(aggregate.Options{})}, value.Int(2), value.Int(0))
	testHistogramZeroNotMutated(t, Histogram{MergeReduce: NewTopK(aggregate.Options{})}, value.NewDict(map[string]value.Value{"foo": value.Double(2.0)}), value.NewDict(nil))
}

func testHistogramStart(t *testing.T, h Histogram, durations []uint32) {
	for _, d := range durations {
		duration := mo.Some(d)
		s, err := Start(h, ftypes.Timestamp(d+10), duration)
		assert.NoError(t, err)
		assert.Equal(t, s, ftypes.Timestamp(10))
		// Duration > end
		s, err = Start(h, ftypes.Timestamp(d-10), duration)
		assert.NoError(t, err)
		assert.Equal(t, s, ftypes.Timestamp(0))
	}
}

func testHistogramZeroNotMutated(t *testing.T, h Histogram, v value.Value, zero value.Value) {
	d := h.Zero()
	m, err := h.Merge(d, v)
	assert.NoError(t, err)
	_, err = h.Reduce([]value.Value{v, m})
	assert.NoError(t, err)
	assert.True(t, h.Zero().Equal(zero))
}
