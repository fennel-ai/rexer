package counter

import (
	"testing"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
)

func TestStart(t *testing.T) {
	durations := []uint64{100, 200}
	badDurations := []uint64{0, 1, 50, 150, 250}
	testHistogramStart(t, average{Durations: durations}, durations, badDurations)
	testHistogramStart(t, list{Durations: durations}, durations, badDurations)
	testHistogramStart(t, rollingMax{Durations: durations}, durations, badDurations)
	testHistogramStart(t, rollingMin{Durations: durations}, durations, badDurations)
	testHistogramStart(t, rollingRate{Durations: durations}, durations, badDurations)
	testHistogramStart(t, rollingStdDev{Durations: durations}, durations, badDurations)
	testHistogramStart(t, rollingSum{Durations: durations}, durations, badDurations)
	testHistogramStart(t, topK{Durations: durations}, durations, badDurations)
}

func TestMergeReduceZeroNotMutated(t *testing.T) {
	durations := []uint64{100, 200}
	testHistogramZeroNotMutated(t, average{Durations: durations}, value.NewList(value.Int(2), value.Int(3)), value.NewList(value.Int(0), value.Int(0)))
	testHistogramZeroNotMutated(t, list{Durations: durations}, value.NewList(value.Int(1), value.Int(2)), value.NewList())
	testHistogramZeroNotMutated(t, rollingMax{Durations: durations}, value.NewList(value.Double(1.0), value.Bool(true)), value.NewList(value.Double(0), value.Bool(true)))
	testHistogramZeroNotMutated(t, rollingMin{Durations: durations}, value.NewList(value.Double(-1.0), value.Bool(true)), value.NewList(value.Double(0), value.Bool(true)))
	testHistogramZeroNotMutated(t, rollingRate{Durations: durations}, value.NewList(value.Double(1.0), value.Double(2.0)), value.NewList(value.Double(0), value.Double(0)))
	testHistogramZeroNotMutated(t, rollingStdDev{Durations: durations}, value.NewList(value.Double(1.0), value.Double(2.0), value.Int(1)), value.NewList(value.Double(0), value.Double(0), value.Int(0)))
	testHistogramZeroNotMutated(t, rollingSum{Durations: durations}, value.Int(2), value.Int(0))
	testHistogramZeroNotMutated(t, topK{Durations: durations}, value.NewDict(map[string]value.Value{"foo": value.Double(2.0)}), value.NewDict(nil))
}

func testHistogramStart(t *testing.T, h Histogram, durations []uint64, badDurations []uint64) {
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
	assert.Equal(t, h.Zero(), zero)
}