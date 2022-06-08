package counter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/value"
)

func SetEqual(t *testing.T, a, b value.List) {
	assert.Equal(t, a.Len(), b.Len())
	m := make(map[string]struct{})
	for i := 0; i < a.Len(); i++ {
		val, _ := a.At(i)
		m[val.String()] = struct{}{}
	}
	for i := 0; i < b.Len(); i++ {
		val, _ := b.At(i)
		_, ok := m[val.String()]
		assert.True(t, ok)
	}
}

func TestList_Reduce(t *testing.T) {
	t.Parallel()
	h := NewList()
	cases := []struct {
		input  []value.Value
		output value.Value
	}{
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(1)),
			value.NewList(value.Int(4), value.Int(2)),
			value.NewList(value.Int(0), value.Int(0))},
			value.NewList(value.Int(0), value.Int(1), value.Int(4), value.Int(2)),
		},
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(0))},
			value.NewList(value.Int(0)),
		},
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(-1)),
			value.NewList(value.Int(2), value.Int(1))},
			value.NewList(value.Int(2), value.Int(1), value.Int(0), value.Int(-1)),
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
		SetEqual(t, c.output.(value.List), found.(value.List))
		// and this works even when one of the elements is zero of the histogram
		c.input = append(c.input, h.Zero())
		assert.NoError(t, err)
		SetEqual(t, c.output.(value.List), found.(value.List))
	}
}

func TestList_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := NewList()
	validCases := []struct {
		input1 value.Value
		input2 value.Value
		output value.Value
	}{
		{
			value.NewList(value.Int(0), value.Int(1)),
			value.NewList(value.Int(1), value.Int(3)),
			value.NewList(value.Int(0), value.Int(1), value.Int(3)),
		},
		{
			value.NewList(value.Int(0), value.Int(0)),
			value.NewList(value.Nil, value.Bool(true)),
			value.NewList(value.Int(0), value.Nil, value.Bool(true)),
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
		SetEqual(t, n.output.(value.List), found.(value.List))
	}
}

func TestList_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := NewList()
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
