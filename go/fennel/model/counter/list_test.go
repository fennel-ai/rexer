package counter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func TestList_Reduce(t *testing.T) {
	t.Parallel()
	h := NewList("some_name", 123)
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

func TestList_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := NewList("some_name", 123)
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

func TestList_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := NewList("some_name", 123)
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

func TestList_Bucketize_Valid(t *testing.T) {
	t.Parallel()
	h := NewList("some_name", 123)
	actions := value.List{}
	expected := make([]Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 5; i++ {
		v := value.Int(1)
		e := value.Int(i)
		d := value.Dict{
			"groupkey":  v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"value":     e,
		}
		assert.NoError(t, actions.Append(d))
		expected = append(expected, Bucket{Value: value.List{e}, Window: ftypes.Window_DAY, Index: 1, Width: 1, Key: v.String()})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_HOUR, Index: uint64(24 + i), Width: 1, Value: value.List{e}})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_MINUTE, Index: uint64(24*60 + i*60), Width: 1, Value: value.List{e}})
	}
	buckets, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestList_Bucketize_Invalid(t *testing.T) {
	t.Parallel()
	h := NewList("some_name", 123)
	cases := [][]value.Dict{
		{value.Dict{}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Int(2)}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Bool(true), "value": value.Int(4)}},
		{value.Dict{"groupkey": value.Int(1), "timestamp": value.Double(1.0), "value": value.Int(3)}},
		{value.Dict{"groupkey": value.Int(1), "value": value.Int(3)}},
		{value.Dict{"timestamp": value.Int(1), "value": value.Int(3)}},
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

func TestList_Start(t *testing.T) {
	h := NewList("some_name", 100)
	assert.Equal(t, h.Start(110), ftypes.Timestamp(10))
	// Duration > end
	assert.Equal(t, h.Start(90), ftypes.Timestamp(0))
}
