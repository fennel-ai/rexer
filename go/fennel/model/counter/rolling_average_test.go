package counter

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRollingAverage_Reduce(t *testing.T) {
	t.Parallel()
	h := RollingAverage{}
	cases := []struct {
		input  []value.Value
		output value.Value
	}{
		{[]value.Value{
			value.List{value.Int(0), value.Int(1)},
			value.List{value.Int(4), value.Int(2)},
			value.List{value.Int(0), value.Int(0)}},
			value.Double(float64(4) / float64(3)),
		},
		{[]value.Value{
			value.List{value.Int(0), value.Int(0)}},
			value.Double(0),
		},
		{[]value.Value{
			value.List{value.Int(0), value.Int(-1)},
			value.List{value.Int(2), value.Int(1)}},
			value.Double(0),
		},
		{[]value.Value{
			value.List{value.Int(-1), value.Int(1)},
			value.List{value.Int(2), value.Int(1)}},
			value.Double(0.5),
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

func TestRollingAverage_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := RollingAverage{}
	validCases := [][]int64{
		{1, 2, 1, 2, 2, 4},
		{1, 2, -1, 2, 0, 4},
		{0, 0, -1, 0, -1, 0},
		{1e12, 1, 1e12, 1, 2e12, 2},
		{1e12, 1, -1e12, 1, 0, 2},
	}
	for _, n := range validCases {
		found, err := h.Merge(value.List{value.Int(n[0]), value.Int(n[1])}, value.List{value.Int(n[2]), value.Int(n[3])})
		assert.NoError(t, err)
		assert.Equal(t, value.List{value.Int(n[4]), value.Int(n[5])}, found)
	}
}

func TestRollingAverage_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := RollingAverage{}
	invalidCases := []struct {
		a value.Value
		b value.Value
	}{
		{value.List{value.Int(0), value.Int(-1), value.Int(4)}, value.List{value.Int(2), value.Int(3)}},
		{value.List{value.Int(0), value.Int(-1)}, value.List{value.Int(2)}},
		{value.List{value.Double(0), value.Int(-1)}, value.List{value.Int(2), value.Double(3)}},
		{value.List{}, value.List{value.Int(2), value.Double(3)}},
		{value.Dict{}, value.List{value.Int(2), value.Double(3)}},
		{value.Nil, value.List{value.Int(2), value.Double(3)}},
	}
	for _, n := range invalidCases {
		_, err := h.Merge(n.a, n.b)
		assert.Error(t, err)

		_, err = h.Merge(n.b, n.a)
		assert.Error(t, err)
	}
}

func TestRollingAverage_Bucketize_Valid(t *testing.T) {
	t.Parallel()
	h := RollingAverage{}
	actions := value.NewTable()
	expected := make([]Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 1; i++ {
		v := value.List{value.Int(i), value.String("hi")}
		d := value.Dict{
			"key":       v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"amount":    value.Int(i),
		}
		assert.NoError(t, actions.Append(d))
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_DAY, Index: 1, Count: value.List{value.Int(i), value.Int(1)}})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_HOUR, Index: uint64(24 + i), Count: value.List{value.Int(i), value.Int(1)}})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_MINUTE, Index: uint64(24*60 + i*60), Count: value.List{value.Int(i), value.Int(1)}})
	}
	buckets, err := h.Bucketize(actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestRollingAverage_Bucketize_Invalid(t *testing.T) {
	t.Parallel()
	h := RollingAverage{}
	cases := [][]value.Dict{
		{value.Dict{}},
		{value.Dict{"key": value.Int(1), "timestamp": value.Int(2)}},
		{value.Dict{"key": value.Int(1), "timestamp": value.Int(2), "amount": value.Nil}},
		{value.Dict{"key": value.Int(1), "timestamp": value.Bool(true), "amount": value.Int(4)}},
		{value.Dict{"key": value.Int(1), "timestamp": value.Double(1.0), "amount": value.Int(3)}},
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
