package counter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/value"
)

func TestMax_Reduce(t *testing.T) {
	t.Parallel()
	h := NewMax()
	cases := []struct {
		input  []value.Value
		output value.Value
	}{{
		makeMaxVals([]value.Value{value.Double(2), value.Int(7), value.Double(5)}, []bool{false, false, false}),
		value.Int(7),
	}, {
		makeMaxVals([]value.Value{value.Double(0)}, []bool{true}),
		value.Double(0),
	},
		{
			makeMaxVals([]value.Value{value.Double(-4), value.Double(-7), value.Double(2)}, []bool{false, false, true}),
			value.Double(-4),
		},
		{
			makeMaxVals([]value.Value{}, []bool{}),
			value.Double(0),
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
	h := NewMax()
	validCases := [][]value.Value{
		makeMaxVals([]value.Value{value.Double(3), value.Double(6), value.Double(6)}, []bool{false, false, false}),
		makeMaxVals([]value.Value{value.Double(-2), value.Double(-5), value.Double(-2)}, []bool{false, false, false}),
		makeMaxVals([]value.Value{value.Double(-9), value.Double(0), value.Double(-9)}, []bool{false, true, false}),
		makeMaxVals([]value.Value{value.Double(0), value.Double(9), value.Double(9)}, []bool{true, false, false}),
		makeMaxVals([]value.Value{value.Double(4), value.Double(5), value.Double(4)}, []bool{false, true, false}),
		makeMaxVals([]value.Value{value.Double(5), value.Double(4), value.Double(4)}, []bool{true, false, false}),
		makeMaxVals([]value.Value{value.Double(5), value.Int(7), value.Int(7)}, []bool{true, false, false}),
	}
	for _, c := range validCases {
		found, err := h.Merge(c[0], c[1])
		assert.NoError(t, err)
		assert.Equal(t, c[2], found)
	}
}

func TestMax_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := NewMax()
	validMaxVals := makeMaxVals(
		[]value.Value{value.Int(-8), value.Int(-2), value.Int(0), value.Double(0), value.Int(5), value.Double(9)},
		[]bool{false, false, false, true, false, false},
	)
	invalidMaxVals := []value.Value{
		value.NewList(value.Int(2), value.Int(1)),
		value.NewList(value.Int(7), value.Bool(false), value.Int(2)),
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

func makeMaxVal(v value.Value, b bool) value.Value {
	return value.NewList(v, value.Bool(b))
}

func makeMaxVals(vs []value.Value, bs []bool) []value.Value {
	ret := make([]value.Value, 0, len(vs))
	for i, v := range vs {
		ret = append(ret, makeMaxVal(v, bs[i]))
	}
	return ret
}
