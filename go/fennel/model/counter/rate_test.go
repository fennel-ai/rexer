package counter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/value"
	"fennel/test"
)

func TestRate_Reduce(t *testing.T) {
	tr, err := test.Tier()
	assert.NoError(t, err)
	cases := []struct {
		r      rollingRate
		input  []value.Value
		output value.Value
	}{
		{NewRate(tr, 1, false),
			[]value.Value{
				value.NewList(value.Int(0), value.Int(1)),
				value.NewList(value.Int(4), value.Int(2)),
				value.NewList(value.Int(0), value.Int(0))},
			value.Double(float64(4) / float64(3)),
		},
		{NewRate(tr, 1, false),
			[]value.Value{
				value.NewList(value.Int(0), value.Int(0))},
			value.Double(0),
		},
		{NewRate(tr, 1, false),
			[]value.Value{
				value.NewList(value.Int(1), value.Int(1)),
				value.NewList(value.Int(34), value.Int(199))},
			value.Double(float64(35) / float64(200)),
		},
		{NewRate(tr, 1, true),
			[]value.Value{
				value.NewList(value.Int(1), value.Int(1)),
				value.NewList(value.Int(34), value.Int(199))},
			value.Double(0.12860441174608936),
		},
		{
			NewRate(tr, 1, false),
			[]value.Value{
				value.NewList(value.Int(0), value.Int(1)),
				value.NewList(value.Int(2), value.Int(1))},
			value.Double(1.),
		},
		{
			NewRate(tr, 1, false),
			[]value.Value{
				value.NewList(value.Int(1e17), value.Int(1e17)),
				value.NewList(value.Int(0), value.Int(1e17))},
			value.Double(0.5),
		},
		// TODO(Mohit): Move this case to _Invalid
		{NewRate(tr, 1, true),
			[]value.Value{
				value.NewList(value.Int(1), value.Int(1)),
				value.NewList(value.Int(2), value.Int(1))},
			value.Double(1.0),
		},
	}
	for _, c := range cases {
		h := c.r
		found, err := h.Reduce(c.input)
		assert.NoError(t, err)
		assert.Equal(t, c.output, found)

		// and this works even when one of the elements is zero of the histogram
		c.input = append(c.input, h.Zero())
		assert.NoError(t, err)
		assert.Equal(t, c.output, found)
	}
}

func TestRate_Reduce_Invalid(t *testing.T) {
	tr, err := test.Tier()
	assert.NoError(t, err)
	cases := []struct {
		r     rollingRate
		input []value.Value
	}{
		{NewRate(tr, 1, false),
			[]value.Value{
				value.NewList(value.Int(-1), value.Int(1)),
				value.NewList(value.Int(0), value.Int(0))},
		},
		{NewRate(tr, 1, false),
			[]value.Value{
				value.NewList(value.Int(0), value.Int(-1))},
		},
		{NewRate(tr, 1, false),
			[]value.Value{
				value.Double(0.5),
				value.NewList(value.Int(34), value.Int(199))},
		},
	}
	for _, c := range cases {
		h := c.r
		_, err := h.Reduce(c.input)
		assert.Error(t, err)

		// and this works even when one of the elements is zero of the histogram
		c.input = append(c.input, h.Zero())
		assert.Error(t, err)
	}
}

func TestRate_Merge_Valid(t *testing.T) {
	tr, err := test.Tier()
	assert.NoError(t, err)
	h := NewRate(tr, 1, false)
	validCases := []struct {
		input1 value.Value
		input2 value.Value
		output value.Value
	}{
		{
			value.NewList(value.Int(0), value.Int(1)),
			value.NewList(value.Int(1), value.Int(3)),
			value.NewList(value.Double(1), value.Double(4)),
		},
		{
			value.NewList(value.Int(0), value.Int(0)),
			value.NewList(value.Int(7), value.Int(11)),
			value.NewList(value.Double(7), value.Double(11)),
		},
		{
			value.NewList(value.Int(1e17), value.Int(1e17)),
			value.NewList(value.Int(1), value.Int(1)),
			value.NewList(value.Double(1+1e17), value.Double(1+1e17)),
		},
	}
	for _, n := range validCases {
		found, err := h.Merge(n.input1, n.input2)
		assert.NoError(t, err)
		assert.Equal(t, n.output, found)
	}
}

func TestRate_Merge_Invalid(t *testing.T) {
	tr, err := test.Tier()
	assert.NoError(t, err)
	h := NewRate(tr, 1, false)
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
			value.List{},
		},
		{
			value.NewList(value.Int(1), value.Int(-1)),
			value.NewList(value.Int(1), value.Int(1)),
		},
	}
	for i, n := range invalidCases {
		_, err := h.Merge(n.input1, n.input2)
		assert.Error(t, err, i)

		_, err = h.Merge(n.input2, n.input1)
		assert.Error(t, err, i)
	}
}
