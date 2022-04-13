package counter

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func TestStddev_Reduce(t *testing.T) {
	t.Parallel()
	h := NewStdDev([]uint64{123})
	cases := []struct {
		input  []value.Value
		output value.Value
	}{{
		makeStddevVals([][]int64{{1, 2, 3}, {4, 5, 6, 7}, {0}}),
		value.Double(stddev([]int64{1, 2, 3, 4, 5, 6, 7, 0})),
	}, {
		makeStddevVals([][]int64{}),
		value.Double(stddev([]int64{})),
	}, {
		makeStddevVals([][]int64{{-7, 2, -9}, {4, -6, -3}, {2, 0, -1}}),
		value.Double(stddev([]int64{-7, 2, -9, 4, -6, -3, 2, 0, -1})),
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
	h := NewStdDev([]uint64{123})
	validCases := [][]int64{
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
			var r []int64
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
	h := NewStdDev([]uint64{123})
	validStddevVals := makeStddevVals([][]int64{
		{-9, -8, -7}, {-6, -5}, {-4, -3, -2, -1, 0}, {}, {0, 1, 2, 3, 4}, {5, 6}, {7, 8, 9},
	})
	invalidStddevVals := []value.Value{
		value.NewList(value.Double(1.0), value.Int(4), value.Int(7)),
		value.NewList(value.Int(2), value.Double(2.0), value.Int(3)),
		value.NewList(value.Int(4), value.Int(16), value.Double(0.0)),
		value.NewList(value.Int(1), value.Int(2), value.Int(3), value.Int(4)),
		value.NewList(value.Int(-4), value.Int(16)),
		value.NewList(value.Int(0)),
		value.NewList(),
		value.NewDict(map[string]value.Value{}),
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

func TestStddev_Bucketize_Valid(t *testing.T) {
	t.Parallel()
	h := NewStdDev([]uint64{123})
	actions := value.NewList()
	expected := make([]Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 5; i++ {
		v := value.NewList(value.Int(i), value.String("hi"))
		d := value.NewDict(map[string]value.Value{
			"groupkey":  v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"value":     value.Int(i),
		})
		count := value.NewList(value.Int(i), value.Int(i*i), value.Int(1))
		actions.Append(d)
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_DAY,
			Index: 1, Width: 1, Value: count})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_MINUTE,
			Index: uint64(24*10 + i*10), Width: 6, Value: count})
	}
	buckets, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestStddev_Bucketize_Invalid(t *testing.T) {
	t.Parallel()
	h := NewStdDev([]uint64{123})
	cases := [][]value.Dict{
		{value.NewDict(map[string]value.Value{})},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "timestamp": value.Int(2)})},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "timestamp": value.Int(2), "value": value.Nil})},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "timestamp": value.Bool(true), "value": value.Int(4)})},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "timestamp": value.Double(1.0), "value": value.Int(3)})},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "value": value.Int(3)})},
		{value.NewDict(map[string]value.Value{"timestamp": value.Int(1), "value": value.Int(3)})},
	}
	for _, test := range cases {
		table := value.NewList()
		for _, d := range test {
			table.Append(d)
		}
		_, err := Bucketize(h, table)
		assert.Error(t, err, fmt.Sprintf("case was: %v", table))
	}
}

func extractFromStddev(vals []int64) (int64, int64, int64) {
	num := int64(len(vals))
	if num == 0 {
		return 0, 0, 0
	}
	var sum, sumsq int64 = 0, 0
	for _, v := range vals {
		sum += v
		sumsq += v * v
	}
	return sum, sumsq, num
}

func stddev(vals []int64) float64 {
	sum, sumsq, num := extractFromStddev(vals)
	if num == 0 {
		return 0
	}
	a := float64(sumsq) / float64(num)
	b := float64(sum) / float64(num)
	return math.Sqrt(a - b*b)
}

func makeStddevVal(vals []int64) value.Value {
	sum, sumsq, num := extractFromStddev(vals)
	return value.NewList(value.Int(sum), value.Int(sumsq), value.Int(num))
}

func makeStddevVals(cases [][]int64) []value.Value {
	ret := make([]value.Value, 0, len(cases))
	for _, c := range cases {
		ret = append(ret, makeStddevVal(c))
	}
	return ret
}
