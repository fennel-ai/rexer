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
	aggTypes := []ftypes.AggType{
		aggregate.SUM,
		aggregate.AVERAGE,
		aggregate.MIN,
		aggregate.MAX,
		aggregate.STDDEV,
		aggregate.LIST,
		aggregate.RATE,
		aggregate.TOPK,
	}
	for _, aggType := range aggTypes {
		t.Run(string(aggType), func(t *testing.T) {
			h, err := ToHistogram(1, aggregate.Options{
				AggType:   aggType,
				Durations: durations,
			})
			assert.NoError(t, err)
			testHistogramStart(t, h, durations)
		})
	}
}

func TestMergeReduceZeroNotMutated(t *testing.T) {
	scenarios := []struct {
		aggType ftypes.AggType
		in      value.Value
		zero    value.Value
	}{
		{aggregate.SUM, value.Int(2), value.Int(0)},
		{aggregate.AVERAGE, value.NewList(value.Int(2), value.Int(3)), value.NewList(value.Int(0), value.Int(0))},
		{aggregate.MAX, value.NewList(value.Double(1.0), value.Bool(true)), value.NewList(value.Double(0), value.Bool(true))},
		{aggregate.MIN, value.NewList(value.Double(-1.0), value.Bool(true)), value.NewList(value.Double(0), value.Bool(true))},
		{aggregate.RATE, value.NewList(value.Double(1.0), value.Double(2.0)), value.NewList(value.Double(0), value.Double(0))},
		{aggregate.STDDEV, value.NewList(value.Double(1.0), value.Double(2.0), value.Int(1)), value.NewList(value.Double(0), value.Double(0), value.Int(0))},
		{aggregate.TOPK, value.NewDict(map[string]value.Value{"foo": value.Double(2.0)}), value.NewDict(nil)},
	}
	for _, s := range scenarios {
		t.Run(string(s.aggType), func(t *testing.T) {
			h, err := ToHistogram(1, aggregate.Options{
				AggType: s.aggType,
			})
			assert.NoError(t, err)
			testHistogramZeroNotMutated(t, h, s.in, s.zero)
		})
	}
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
