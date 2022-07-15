package counter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/aggregate"
	"fennel/lib/value"
)

func TestRollingAverage_Reduce(t *testing.T) {
	t.Parallel()
	h := NewAverage(aggregate.Options{})
	cases := []struct {
		input  []value.Value
		output value.Value
	}{
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(1)),
			value.NewList(value.Int(4), value.Int(2)),
			value.NewList(value.Int(0), value.Int(0))},
			value.Double(float64(4) / float64(3)),
		},
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(0))},
			value.Double(0),
		},
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(-1)),
			value.NewList(value.Int(2), value.Int(1))},
			value.Double(0),
		},
		{[]value.Value{
			value.NewList(value.Int(-1), value.Int(1)),
			value.NewList(value.Int(2), value.Int(1))},
			value.Double(0.5),
		},
	}
	for _, c := range cases {
		found, err := h.Reduce(c.input)
		assert.NoError(t, err)
		assert.Equal(t, c.output, found)

		// and this works even when one of the elements is zero of the histogram
		c.input = append(c.input, h.Zero())
		assert.NoError(t, err)
		assert.Equal(t, c.output, found)
	}
}

func TestRollingAverage_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := NewAverage(aggregate.Options{})
	validCases := [][]value.Value{
		{value.Int(1), value.Int(2), value.Int(1), value.Int(2), value.Int(2), value.Int(4)},
		{value.Int(1), value.Int(2), value.Double(2), value.Int(2), value.Double(3), value.Int(4)},
		{value.Int(1), value.Int(2), value.Int(-1), value.Int(2), value.Int(0), value.Int(4)},
		{value.Int(0), value.Int(0), value.Double(0), value.Int(0), value.Double(0), value.Int(0)},
		{value.Int(1e12), value.Int(1), value.Int(1e12), value.Int(1), value.Int(2e12), value.Int(2)},
		{value.Int(1e12), value.Int(1), value.Int(-1e12), value.Int(1), value.Int(0), value.Int(2)},
		{value.Int(1), value.Int(2), value.Int(-2), value.Int(2), value.Int(-1), value.Int(4)},
		{value.Int(1), value.Int(2), value.Double(1.52), value.Int(3), value.Double(2.52), value.Int(5)},
	}
	for _, n := range validCases {
		found, err := h.Merge(value.NewList(n[0], n[1]), value.NewList(n[2], n[3]))
		assert.NoError(t, err)
		assert.Equal(t, value.NewList(n[4], n[5]), found)
	}
}

func TestRollingAverage_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := NewAverage(aggregate.Options{})
	invalidCases := []struct {
		a value.Value
		b value.Value
	}{
		{value.NewList(value.Int(0), value.Int(-1), value.Int(4)), value.NewList(value.Int(2), value.Int(3))},
		{value.NewList(value.Int(0), value.Int(-1)), value.NewList(value.Int(2))},
		{value.NewList(value.Double(0), value.Int(-1)), value.NewList(value.Int(2), value.Double(3))},
		{value.NewList(), value.NewList(value.Int(2), value.Double(3))},
		{value.NewDict(nil), value.NewList(value.Int(2), value.Double(3))},
		{value.Nil, value.NewList(value.Int(2), value.Double(3))},
	}
	for _, n := range invalidCases {
		_, err := h.Merge(n.a, n.b)
		assert.Error(t, err)

		_, err = h.Merge(n.b, n.a)
		assert.Error(t, err)
	}
}
