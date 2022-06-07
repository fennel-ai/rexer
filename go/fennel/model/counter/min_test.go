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
		makeMinVals([]value.Value{value.Int(2), value.Double(7), value.Double(5)}, []bool{false, false, false}),
		value.Int(2),
	}, {
		makeMinVals([]value.Value{value.Int(0)}, []bool{false}),
		value.Int(0),
	},
		{
			makeMinVals([]value.Value{value.Double(-4), value.Double(-7), value.Double(-12)}, []bool{false, false, true}),
			value.Double(-7),
		},
		{
			makeMinVals([]value.Value{}, []bool{}),
			value.Double(0),
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
		makeMinVals([]value.Value{value.Double(3), value.Double(6), value.Double(3)}, []bool{false, false, false}),
		makeMinVals([]value.Value{value.Double(-2), value.Double(-5), value.Double(-5)}, []bool{false, false, false}),
		makeMinVals([]value.Value{value.Double(-9), value.Double(0), value.Double(-9)}, []bool{false, true, false}),
		makeMinVals([]value.Value{value.Double(0), value.Double(9), value.Double(9)}, []bool{true, false, false}),
		makeMinVals([]value.Value{value.Double(5), value.Double(4), value.Double(5)}, []bool{false, true, false}),
		makeMinVals([]value.Value{value.Double(4), value.Double(5), value.Double(5)}, []bool{true, false, false}),
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
		[]value.Value{value.Int(-8), value.Double(-2), value.Int(0), value.Int(0), value.Double(5), value.Int(9)},
		[]bool{false, false, false, true, false, false},
	)
	invalidMinVals := []value.Value{
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
	expVals := make([]value.Value, 0)
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
			Index: 1, Width: 1})
		expVals = append(expVals, value.NewList(value.Int(i), value.Bool(false)))
		expected = append(expected, counter.Bucket{Key: v.String(), Window: ftypes.Window_MINUTE,
			Index: uint32(24*10 + i*10), Width: 6})
		expVals = append(expVals, value.NewList(value.Int(i), value.Bool(false)))
	}
	buckets, vals, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
	assert.ElementsMatch(t, expVals, vals)
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
		_, _, err := Bucketize(h, table)
		assert.Error(t, err, fmt.Sprintf("case was: %v", table))
	}
}

func makeMinVal(v value.Value, b bool) value.Value {
	return value.NewList(v, value.Bool(b))
}

func makeMinVals(vs []value.Value, bs []bool) []value.Value {
	ret := make([]value.Value, 0, len(vs))
	for i, v := range vs {
		ret = append(ret, makeMinVal(v, bs[i]))
	}
	return ret
}
