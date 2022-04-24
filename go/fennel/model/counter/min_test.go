package counter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func TestMin_Reduce(t *testing.T) {
	t.Parallel()
	h := NewMin([]uint64{123})
	cases := []struct {
		input  []value.Value
		output value.Value
	}{{
		makeMinVals([]int64{2, 7, 5}, []bool{false, false, false}),
		value.Int(2),
	}, {
		makeMinVals([]int64{0}, []bool{true}),
		value.Int(0),
	},
		{
			makeMinVals([]int64{-4, -7, -12}, []bool{false, false, true}),
			value.Int(-7),
		},
		{
			makeMinVals([]int64{}, []bool{}),
			value.Int(0),
		},
	}
	for _, c := range cases {
		found, err := h.Reduce(c.input)
		assert.NoError(t, err, c.input)
		assert.Equal(t, c.output, found)

		// and this works even when one of the elements is zero of the histogram
		c.input = append(c.input, h.Zero())
		assert.NoError(t, err)
		assert.Equal(t, c.output, found)
	}
}

func TestMin_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := NewMin([]uint64{123})
	validCases := [][]value.Value{
		makeMinVals([]int64{3, 6, 3}, []bool{false, false, false}),
		makeMinVals([]int64{-2, -5, -5}, []bool{false, false, false}),
		makeMinVals([]int64{-9, 0, -9}, []bool{false, true, false}),
		makeMinVals([]int64{0, 9, 9}, []bool{true, false, false}),
		makeMinVals([]int64{5, 4, 5}, []bool{false, true, false}),
		makeMinVals([]int64{4, 5, 5}, []bool{true, false, false}),
	}
	for _, c := range validCases {
		found, err := h.Merge(c[0], c[1])
		assert.NoError(t, err)
		assert.Equal(t, c[2], found)
	}
}

func TestMin_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := NewMin([]uint64{123})
	validMinVals := makeMinVals(
		[]int64{-8, -2, 0, 0, 5, 9},
		[]bool{false, false, false, true, false, false},
	)
	invalidMinVals := []value.Value{
		value.NewList(value.Double(2.0), value.Bool(false)),
		value.NewList(value.Int(2), value.Int(1)),
		value.NewList(value.Int(7), value.Bool(false), value.Int(2)),
		value.NewList(),
		value.NewDict(nil),
		value.Nil,
	}
	var allMinVals []value.Value
	allMinVals = append(allMinVals, validMinVals...)
	allMinVals = append(allMinVals, invalidMinVals...)
	for _, nv := range invalidMinVals {
		for _, v := range allMinVals {
			_, err := h.Merge(v, nv)
			assert.Error(t, err)

			_, err = h.Merge(nv, v)
			assert.Error(t, err)
		}
	}
}

func TestMin_Bucketize_Valid(t *testing.T) {
	t.Parallel()
	h := NewMin([]uint64{123})
	actions := value.List{}
	expected := make([]counter.Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 5; i++ {
		v := value.NewList(value.Int(i), value.String("hi"))
		d := value.NewDict(map[string]value.Value{
			"groupkey":  v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"value":     value.Int(i),
		})
		actions.Append(d)
		expected = append(expected, counter.Bucket{Key: v.String(), Window: ftypes.Window_DAY,
			Index: 1, Width: 1, Value: value.NewList(value.Int(i), value.Bool(false))})
		expected = append(expected, counter.Bucket{Key: v.String(), Window: ftypes.Window_MINUTE,
			Index: uint64(24*10 + i*10), Width: 6, Value: value.NewList(value.Int(i), value.Bool(false))})
	}
	buckets, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestMin_Bucketize_Invalid(t *testing.T) {
	t.Parallel()
	h := NewMin([]uint64{123})
	cases := [][]value.Dict{
		{value.Dict{}},
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

func makeMinVal(v int64, b bool) value.Value {
	return value.NewList(value.Int(v), value.Bool(b))
}

func makeMinVals(vs []int64, bs []bool) []value.Value {
	ret := make([]value.Value, 0, len(vs))
	for i, v := range vs {
		ret = append(ret, makeMinVal(v, bs[i]))
	}
	return ret
}
