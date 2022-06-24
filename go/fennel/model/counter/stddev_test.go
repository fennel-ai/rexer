package counter

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/aggregate"
	"fennel/lib/value"
)

func TestStddev_Reduce(t *testing.T) {
	t.Parallel()
	h := NewStdDev(aggregate.Options{})
	cases := []struct {
		input  []value.Value
		output value.Value
	}{{
		makeStddevVals([][]float64{{1, 2, 3}, {4, 5, 6, 7}, {0}}),
		value.Double(stddev([]float64{1, 2, 3, 4, 5, 6, 7, 0})),
	}, {
		makeStddevVals([][]float64{}),
		value.Double(stddev([]float64{})),
	}, {
		makeStddevVals([][]float64{{-7, 2, -9}, {4, -6, -3}, {2, 0, -1}}),
		value.Double(stddev([]float64{-7, 2, -9, 4, -6, -3, 2, 0, -1})),
	}}
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

func TestStddev_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := NewStdDev(aggregate.Options{})
	validCases := [][]float64{
		{4, -2, 9, -11, 3},
		{2, -7, 6, 0},
		{4254, -9823, 8792},
		{-9272, 3799, -9237},
		{},
		{-2},
		{-1},
	}
	for _, c1 := range validCases {
		for _, c2 := range validCases {
			var r []float64
			r = append(r, c1...)
			r = append(r, c2...)
			found, err := h.Merge(makeStddevVal(c1), makeStddevVal(c2))
			assert.NoError(t, err)
			expected := makeStddevVal(r)
			assert.Equal(t, expected, found)
		}
	}
}

func TestStddev_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := NewStdDev(aggregate.Options{})
	validStddevVals := makeStddevVals([][]float64{
		{-9, -8, -7}, {-6, -5}, {-4, -3, -2, -1, 0}, {}, {0, 1, 2, 3, 4}, {5, 6}, {7, 8, 9},
	})
	invalidStddevVals := []value.Value{
		value.NewList(value.Int(4), value.Int(16), value.Double(0.0)),
		value.NewList(value.Int(1), value.Int(2), value.Int(3), value.Int(4)),
		value.NewList(value.Int(-4), value.Int(16)),
		value.NewList(value.Int(0)),
		value.NewList(),
		value.NewDict(nil),
		value.Nil,
	}
	var allStddevVals []value.Value
	allStddevVals = append(allStddevVals, validStddevVals...)
	allStddevVals = append(allStddevVals, invalidStddevVals...)
	for _, nv := range invalidStddevVals {
		for _, v := range allStddevVals {
			_, err := h.Merge(v, nv)
			assert.Error(t, err)

			_, err = h.Merge(nv, v)
			assert.Error(t, err)
		}
	}
}

func extractFromStddev(vals []float64) (float64, float64, int64) {
	num := int64(len(vals))
	if num == 0 {
		return 0, 0, 0
	}
	var sum, sumsq float64 = 0, 0
	for _, v := range vals {
		sum += v
		sumsq += v * v
	}
	return sum, sumsq, num
}

func stddev(vals []float64) float64 {
	sum, sumsq, num := extractFromStddev(vals)
	if num == 0 {
		return 0
	}
	a := float64(sumsq) / float64(num)
	b := float64(sum) / float64(num)
	return math.Sqrt(a - b*b)
}

func makeStddevVal(vals []float64) value.Value {
	sum, sumsq, num := extractFromStddev(vals)
	return value.NewList(value.Double(sum), value.Double(sumsq), value.Int(num))
}

func makeStddevVals(cases [][]float64) []value.Value {
	ret := make([]value.Value, 0, len(cases))
	for _, c := range cases {
		ret = append(ret, makeStddevVal(c))
	}
	return ret
}
