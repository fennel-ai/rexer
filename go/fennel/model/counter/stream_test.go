package counter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func TestStream_Reduce(t *testing.T) {
	t.Parallel()
	h := Stream{}
	cases := []struct {
		input  []value.Value
		output value.Value
	}{
		{[]value.Value{
			value.List{value.Int(0), value.Int(1)},
			value.List{value.Int(4), value.Int(2)},
			value.List{value.Int(0), value.Int(0)}},
			value.List{value.Int(0), value.Int(1), value.Int(4), value.Int(2), value.Int(0), value.Int(0)},
		},
		{[]value.Value{
			value.List{value.Int(0), value.Int(0)}},
			value.List{value.Int(0), value.Int(0)},
		},
		{[]value.Value{
			value.List{value.Int(0), value.Int(-1)},
			value.List{value.Int(2), value.Int(1)}},
			value.List{value.Int(0), value.Int(-1), value.Int(2), value.Int(1)},
		},
		{[]value.Value{
			value.List{value.Int(1e17), value.Int(-1e17)},
			value.List{value.Int(2), value.Int(1)}},
			value.List{value.Int(1e17), value.Int(-1e17), value.Int(2), value.Int(1)},
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

func TestStream_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := Stream{}
	validCases := []struct {
		input1 value.Value
		input2 value.Value
		output value.Value
	}{
		{
			value.List{value.Int(0), value.Int(1)},
			value.List{value.Int(1), value.Int(3)},
			value.List{value.Int(0), value.Int(1), value.Int(1), value.Int(3)},
		},
		{
			value.List{value.Int(0), value.Int(0)},
			value.List{value.Nil, value.Bool(true)},
			value.List{value.Int(0), value.Int(0), value.Nil, value.Bool(true)},
		},
		{
			value.List{value.Int(0), value.Int(-1)},
			value.List{value.Int(2), value.List{value.Int(3)}},
			value.List{value.Int(0), value.Int(-1), value.Int(2), value.List{value.Int(3)}},
		},
		{
			value.List{value.Int(1e17), value.Int(-1e17)},
			value.List{},
			value.List{value.Int(1e17), value.Int(-1e17)},
		},
		{
			value.List{},
			value.List{},
			value.List{},
		},
	}
	for _, n := range validCases {
		found, err := h.Merge(n.input1, n.input2)
		assert.NoError(t, err)
		assert.Equal(t, n.output, found)
	}
}

func TestStream_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := RollingAverage{}
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
	}
	for _, n := range invalidCases {
		_, err := h.Merge(n.input1, n.input2)
		assert.Error(t, err)

		_, err = h.Merge(n.input2, n.input1)
		assert.Error(t, err)
	}
}

func TestStream_Bucketize_Valid(t *testing.T) {
	t.Parallel()
	h := Stream{}
	actions := value.NewTable()
	expected := make([]Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 5; i++ {
		v := value.Int(1)
		e := value.Int(i)
		d := value.Dict{
			"key":       v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"element":   e,
		}
		assert.NoError(t, actions.Append(d))
		expected = append(expected, Bucket{Count: value.List{e}, Window: ftypes.Window_DAY, Index: 1, Key: v.String()})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_HOUR, Index: uint64(24 + i), Count: value.List{e}})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_MINUTE, Index: uint64(24*60 + i*60), Count: value.List{e}})
	}
	buckets, err := h.Bucketize(actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestStream_Bucketize_Invalid(t *testing.T) {
	t.Parallel()
	h := RollingAverage{}
	cases := [][]value.Dict{
		{value.Dict{}},
		{value.Dict{"key": value.Int(1), "timestamp": value.Int(2)}},
		{value.Dict{"key": value.Int(1), "timestamp": value.Bool(true), "element": value.Int(4)}},
		{value.Dict{"key": value.Int(1), "timestamp": value.Double(1.0), "element": value.Int(3)}},
		{value.Dict{"key": value.Int(1), "amount": value.Int(3)}},
		{value.Dict{"timestamp": value.Int(1), "amount": value.Int(3)}},
	}
	for _, test := range cases {
		table := value.NewTable()
		for _, d := range test {
			assert.NoError(t, table.Append(d))
		}
		_, err := h.Bucketize(table)
		assert.Error(t, err, fmt.Sprintf("case was: %v", table))
	}
}
