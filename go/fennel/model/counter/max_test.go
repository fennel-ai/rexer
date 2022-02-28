package counter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func TestMax_Reduce(t *testing.T) {
	t.Parallel()
	h := Max{}
	cases := []struct {
		input  []value.Value
		output value.Value
	}{{
		makeMaxVals([]int64{2, 7, 5}, []bool{false, false, false}),
		value.Int(7),
	}, {
		makeMaxVals([]int64{0}, []bool{true}),
		value.Int(0),
	},
		{
			makeMaxVals([]int64{-4, -7, 2}, []bool{false, false, true}),
			value.Int(-4),
		},
		{
			makeMaxVals([]int64{}, []bool{}),
			value.Int(0),
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

func TestMax_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := Max{}
	validCases := [][]value.Value{
		makeMaxVals([]int64{3, 6, 6}, []bool{false, false, false}),
		makeMaxVals([]int64{-2, -5, -2}, []bool{false, false, false}),
		makeMaxVals([]int64{-9, 0, -9}, []bool{false, true, false}),
		makeMaxVals([]int64{0, 9, 9}, []bool{true, false, false}),
		makeMaxVals([]int64{4, 5, 4}, []bool{false, true, false}),
		makeMaxVals([]int64{5, 4, 4}, []bool{true, false, false}),
	}
	for _, c := range validCases {
		found, err := h.Merge(c[0], c[1])
		assert.NoError(t, err)
		assert.Equal(t, c[2], found)
	}
}

func TestMax_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := Max{}
	validMaxVals := makeMaxVals(
		[]int64{-8, -2, 0, 0, 5, 9},
		[]bool{false, false, false, true, false, false},
	)
	invalidMaxVals := []value.Value{
		value.List{value.Double(2.0), value.Bool(false)},
		value.List{value.Int(2), value.Int(1)},
		value.List{value.Int(7), value.Bool(false), value.Int(2)},
		value.List{},
		value.Dict{},
		value.Nil,
	}
	var allMaxVals []value.Value
	allMaxVals = append(allMaxVals, validMaxVals...)
	allMaxVals = append(allMaxVals, invalidMaxVals...)
	for _, nv := range invalidMaxVals {
		for _, v := range allMaxVals {
			_, err := h.Merge(v, nv)
			assert.Error(t, err)

			_, err = h.Merge(nv, v)
			assert.Error(t, err)
		}
	}
}

func TestMax_Bucketize_Valid(t *testing.T) {
	t.Parallel()
	h := Max{}
	actions := value.NewTable()
	expected := make([]Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 5; i++ {
		v := value.List{value.Int(i), value.String("hi")}
		d := value.Dict{
			"groupkey":  v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"value":     value.Int(i),
		}
		assert.NoError(t, actions.Append(d))
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_DAY,
			Index: 1, Count: value.List{value.Int(i), value.Bool(false)}})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_HOUR,
			Index: uint64(24 + i), Count: value.List{value.Int(i), value.Bool(false)}})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_MINUTE,
			Index: uint64(24*60 + i*60), Count: value.List{value.Int(i), value.Bool(false)}})
	}
	buckets, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestMax_Bucketize_Invalid(t *testing.T) {
	t.Parallel()
	h := Max{}
	cases := [][]value.Dict{
		{value.Dict{}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Int(2)}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Int(2), "value": value.Nil}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Bool(true), "value": value.Int(4)}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Double(1.0), "value": value.Int(3)}},
		{value.Dict{"groupkey": value.Int(1), "value": value.Int(3)}},
		{value.Dict{"timestamp": value.Int(1), "value": value.Int(3)}},
	}
	for _, test := range cases {
		table := value.NewTable()
		for _, d := range test {
			assert.NoError(t, table.Append(d))
		}
		_, err := Bucketize(h, table)
		assert.Error(t, err, fmt.Sprintf("case was: %v", table))
	}
}

func TestMax_Start(t *testing.T) {
	h := Max{Duration: 100}
	assert.Equal(t, h.Start(110), ftypes.Timestamp(10))
	// Duration > end
	assert.Equal(t, h.Start(90), ftypes.Timestamp(0))
}

func makeMaxVal(v int64, b bool) value.Value {
	return value.List{value.Int(v), value.Bool(b)}
}

func makeMaxVals(vs []int64, bs []bool) []value.Value {
	ret := make([]value.Value, 0, len(vs))
	for i, v := range vs {
		ret = append(ret, makeMaxVal(v, bs[i]))
	}
	return ret
}
