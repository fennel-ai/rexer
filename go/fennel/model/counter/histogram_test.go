package counter

import (
	"testing"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {
	durations := []uint32{100, 200}
	badDurations := []uint32{0, 1, 50, 150, 250}
	testHistogramStart(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: average{}}, durations, badDurations)
	testHistogramStart(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: list{}}, durations, badDurations)
	testHistogramStart(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: rollingMax{}}, durations, badDurations)
	testHistogramStart(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: rollingMin{}}, durations, badDurations)
	testHistogramStart(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: rollingRate{}}, durations, badDurations)
	testHistogramStart(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: rollingStdDev{}}, durations, badDurations)
	testHistogramStart(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: rollingSum{}}, durations, badDurations)
	testHistogramStart(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: topK{}}, durations, badDurations)
}

func TestMergeReduceZeroNotMutated(t *testing.T) {
	durations := []uint32{100, 200}
	testHistogramZeroNotMutated(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: average{}}, value.NewList(value.Int(2), value.Int(3)), value.NewList(value.Int(0), value.Int(0)))
	testHistogramZeroNotMutated(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: list{}}, value.NewList(value.Int(1), value.Int(2)), value.NewList())
	testHistogramZeroNotMutated(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: rollingMax{}}, value.NewList(value.Double(1.0), value.Bool(true)), value.NewList(value.Double(0), value.Bool(true)))
	testHistogramZeroNotMutated(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: rollingMin{}}, value.NewList(value.Double(-1.0), value.Bool(true)), value.NewList(value.Double(0), value.Bool(true)))
	testHistogramZeroNotMutated(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: rollingRate{}}, value.NewList(value.Double(1.0), value.Double(2.0)), value.NewList(value.Double(0), value.Double(0)))
	testHistogramZeroNotMutated(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: rollingStdDev{}}, value.NewList(value.Double(1.0), value.Double(2.0), value.Int(1)), value.NewList(value.Double(0), value.Double(0), value.Int(0)))
	testHistogramZeroNotMutated(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: rollingSum{}}, value.Int(2), value.Int(0))
	testHistogramZeroNotMutated(t, Histogram{Options: aggregate.Options{Durations: durations}, MergeReduce: topK{}}, value.NewDict(map[string]value.Value{"foo": value.Double(2.0)}), value.NewDict(nil))
}

func testHistogramStart(t *testing.T, h Histogram, durations []uint32, badDurations []uint32) {
	for _, d := range durations {
		kwargs := value.NewDict(map[string]value.Value{"duration": value.Int(d)})
		s, err := h.Start(ftypes.Timestamp(d+10), kwargs)
		assert.NoError(t, err)
		assert.Equal(t, s, ftypes.Timestamp(10))
		// Duration > end
		s, err = h.Start(ftypes.Timestamp(d-10), kwargs)
		assert.NoError(t, err)
		assert.Equal(t, s, ftypes.Timestamp(0))
	}
	for _, bd := range badDurations {
		kwargs := value.NewDict(map[string]value.Value{"duration": value.Int(bd)})
		_, err := h.Start(200, kwargs)
		assert.Error(t, err)
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
