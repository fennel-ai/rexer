package counter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func TestRate_Reduce(t *testing.T) {
	t.Parallel()
	cases := []struct {
		h      Histogram
		input  []value.Value
		output value.Value
	}{
		{NewRate("some", 100, false),
			[]value.Value{
				value.List{value.Int(0), value.Int(1)},
				value.List{value.Int(4), value.Int(2)},
				value.List{value.Int(0), value.Int(0)}},
			value.Double(float64(4) / float64(3)),
		},
		{NewRate("some", 100, false),
			[]value.Value{
				value.List{value.Int(0), value.Int(0)}},
			value.Double(0),
		},
		{NewRate("some", 100, false),
			[]value.Value{
				value.List{value.Int(1), value.Int(1)},
				value.List{value.Int(34), value.Int(199)}},
			value.Double(float64(35) / float64(200)),
		},
		{NewRate("some", 100, true),
			[]value.Value{
				value.List{value.Int(1), value.Int(1)},
				value.List{value.Int(34), value.Int(199)}},
			value.Double(0.12860441174608936),
		},
		{
			NewRate("some", 100, false),
			[]value.Value{
				value.List{value.Int(0), value.Int(1)},
				value.List{value.Int(2), value.Int(1)}},
			value.Double(1.),
		},
		{
			NewRate("some", 100, false),
			[]value.Value{
				value.List{value.Int(1e17), value.Int(1e17)},
				value.List{value.Int(0), value.Int(1e17)}},
			value.Double(0.5),
		},
	}
	for _, c := range cases {
		h := c.h
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
	t.Parallel()
	cases := []struct {
		h     Histogram
		input []value.Value
	}{
		{NewRate("some", 100, false),
			[]value.Value{
				value.List{value.Int(-1), value.Int(1)},
				value.List{value.Int(0), value.Int(0)}},
		},
		{NewRate("some", 100, false),
			[]value.Value{
				value.List{value.Int(0), value.Int(-1)}},
		},
		{NewRate("some", 100, false),
			[]value.Value{
				value.Double(0.5),
				value.List{value.Int(34), value.Int(199)}},
		},
		{NewRate("some", 100, true),
			[]value.Value{
				value.List{value.Int(1), value.Int(1)},
				value.List{value.Int(2), value.Int(1)}},
		},
	}
	for _, c := range cases {
		h := c.h
		_, err := h.Reduce(c.input)
		assert.Error(t, err)

		// and this works even when one of the elements is zero of the histogram
		c.input = append(c.input, h.Zero())
		assert.Error(t, err)
	}
}

func TestRate_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := NewRate("some", 100, false)
	validCases := []struct {
		input1 value.Value
		input2 value.Value
		output value.Value
	}{
		{
			value.List{value.Int(0), value.Int(1)},
			value.List{value.Int(1), value.Int(3)},
			value.List{value.Int(1), value.Int(4)},
		},
		{
			value.List{value.Int(0), value.Int(0)},
			value.List{value.Int(7), value.Int(11)},
			value.List{value.Int(7), value.Int(11)},
		},
		{
			value.List{value.Int(1e17), value.Int(1e17)},
			value.List{value.Int(1), value.Int(1)},
			value.List{value.Int(1 + 1e17), value.Int(1 + 1e17)},
		},
	}
	for _, n := range validCases {
		found, err := h.Merge(n.input1, n.input2)
		assert.NoError(t, err)
		assert.Equal(t, n.output, found)
	}
}

func TestRate_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := NewRate("some", 100, false)
	invalidCases := []struct {
		input1 value.Value
		input2 value.Value
	}{
		{
			value.List{value.Int(0), value.Int(1)},
			value.Int(0),
		},
		{
			value.Nil,
			value.Bool(false),
		},
		{
			value.List{},
			value.Dict{},
		},
		{
			value.Double(0.0),
			value.List{},
		},
		{
			value.List{value.Int(1), value.Int(-1)},
			value.List{value.Int(1), value.Int(1)},
		},
	}
	for i, n := range invalidCases {
		_, err := h.Merge(n.input1, n.input2)
		assert.Error(t, err, i)

		_, err = h.Merge(n.input2, n.input1)
		assert.Error(t, err, i)
	}
}

func TestRate_Bucketize_Valid(t *testing.T) {
	t.Parallel()
	h := NewRate("some", 100, false)
	actions := value.List{}
	expected := make([]Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 5; i++ {
		v := value.Int(1)
		e := value.List{value.Int(i), value.Int(i)}
		d := value.Dict{
			"groupkey":  v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"value":     e,
		}
		assert.NoError(t, actions.Append(d))
		expected = append(expected, Bucket{Value: e, Window: ftypes.Window_DAY, Index: 1, Width: 1, Key: v.String()})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_MINUTE, Width: 6, Index: uint64(24*10 + i*10), Value: e})
	}
	buckets, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestRate_Bucketize_Invalid(t *testing.T) {
	t.Parallel()
	h := NewRate("some", 100, false)
	cases := [][]value.Dict{
		{value.Dict{}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Int(2)}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Bool(true), "value": value.Int(4)}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Double(1.0), "value": value.Int(3)}},
		{value.Dict{"groupkey": value.Int(1), "value": value.Int(3)}},
		{value.Dict{"timestamp": value.Int(1), "value": value.Int(3)}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Int(2), "value": value.Int(1)}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Int(2), "value": value.Double(1)}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Int(2), "value": value.List{value.Int(1)}}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Int(2), "value": value.List{value.Int(1), value.Int(2), value.Int(3)}}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Int(2), "value": value.List{value.Double(1), value.Double(2)}}},
	}
	for _, test := range cases {
		table := value.List{}
		for _, d := range test {
			assert.NoError(t, table.Append(d))
		}
		_, err := Bucketize(h, table)
		assert.Error(t, err, fmt.Sprintf("case was: %v", table))
	}
}

func TestRate_Start(t *testing.T) {
	h := rollingRate{Duration: 100}
	assert.Equal(t, h.Start(110), ftypes.Timestamp(10))
	// Duration > end
	assert.Equal(t, h.Start(90), ftypes.Timestamp(0))
}
