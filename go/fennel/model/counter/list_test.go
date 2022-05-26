package counter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/value"
)

func TestList_Reduce(t *testing.T) {
	t.Parallel()
	h := NewList([]uint64{123})
	cases := []struct {
		input  []value.Value
		output value.Value
	}{
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(1)),
			value.NewList(value.Int(4), value.Int(2)),
			value.NewList(value.Int(0), value.Int(0))},
			value.NewList(value.Int(0), value.Int(1), value.Int(4), value.Int(2), value.Int(0), value.Int(0)),
		},
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(0))},
			value.NewList(value.Int(0), value.Int(0)),
		},
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(-1)),
			value.NewList(value.Int(2), value.Int(1))},
			value.NewList(value.Int(0), value.Int(-1), value.Int(2), value.Int(1)),
		},
		{[]value.Value{
			value.NewList(value.Int(1e17), value.Int(-1e17)),
			value.NewList(value.Int(2), value.Int(1))},
			value.NewList(value.Int(1e17), value.Int(-1e17), value.Int(2), value.Int(1)),
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

func TestList_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := NewList([]uint64{123})
	validCases := []struct {
		input1 value.Value
		input2 value.Value
		output value.Value
	}{
		{
			value.NewList(value.Int(0), value.Int(1)),
			value.NewList(value.Int(1), value.Int(3)),
			value.NewList(value.Int(0), value.Int(1), value.Int(1), value.Int(3)),
		},
		{
			value.NewList(value.Int(0), value.Int(0)),
			value.NewList(value.Nil, value.Bool(true)),
			value.NewList(value.Int(0), value.Int(0), value.Nil, value.Bool(true)),
		},
		{
			value.NewList(value.Int(0), value.Int(-1)),
			value.NewList(value.Int(2), value.NewList(value.Int(3))),
			value.NewList(value.Int(0), value.Int(-1), value.Int(2), value.NewList(value.Int(3))),
		},
		{
			value.NewList(value.Int(1e17), value.Int(-1e17)),
			value.NewList(),
			value.NewList(value.Int(1e17), value.Int(-1e17)),
		},
		{
			value.NewList(),
			value.NewList(),
			value.NewList(),
		},
	}
	for _, n := range validCases {
		found, err := h.Merge(n.input1, n.input2)
		assert.NoError(t, err)
		assert.Equal(t, n.output, found)
	}
}

func TestList_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := NewList([]uint64{123})
	invalidCases := []struct {
		input1 value.Value
		input2 value.Value
	}{
		{
			value.NewList(value.Int(0), value.Int(1)),
			value.Int(0),
		},
		{
			value.Nil,
			value.Bool(false),
		},
		{
			value.NewList(),
			value.NewDict(nil),
		},
		{
			value.Double(0.0),
			value.NewList(),
		},
	}
	for _, n := range invalidCases {
		_, err := h.Merge(n.input1, n.input2)
		assert.Error(t, err)

		_, err = h.Merge(n.input2, n.input1)
		assert.Error(t, err)
	}
}
