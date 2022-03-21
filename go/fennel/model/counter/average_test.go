package counter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func TestRollingAverage_Reduce(t *testing.T) {
	t.Parallel()
	h := NewAverage("somename", 100)
	cases := []struct {
		input  []value.Value
		output value.Value
	}{
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(1)),
			value.NewList(value.Int(4), value.Int(2)),
			value.NewList(value.Int(0), value.Int(0))},
			value.Double(float64(4) / float64(3)),
		},
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(0))},
			value.Double(0),
		},
		{[]value.Value{
			value.NewList(value.Int(0), value.Int(-1)),
			value.NewList(value.Int(2), value.Int(1))},
			value.Double(0),
		},
		{[]value.Value{
			value.NewList(value.Int(-1), value.Int(1)),
			value.NewList(value.Int(2), value.Int(1))},
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
	h := NewAverage("somename", 100)
	validCases := [][]int64{
		{1, 2, 1, 2, 2, 4},
		{1, 2, -1, 2, 0, 4},
		{0, 0, -1, 0, -1, 0},
		{1e12, 1, 1e12, 1, 2e12, 2},
		{1e12, 1, -1e12, 1, 0, 2},
	}
	for _, n := range validCases {
		found, err := h.Merge(value.NewList(value.Int(n[0]), value.Int(n[1])), value.NewList(value.Int(n[2]), value.Int(n[3])))
		assert.NoError(t, err)
		assert.Equal(t, value.NewList(value.Int(n[4]), value.Int(n[5])), found)
	}
}

func TestRollingAverage_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := NewAverage("somename", 100)
	invalidCases := []struct {
		a value.Value
		b value.Value
	}{
		{value.NewList(value.Int(0), value.Int(-1), value.Int(4)), value.NewList(value.Int(2), value.Int(3))},
		{value.NewList(value.Int(0), value.Int(-1)), value.NewList(value.Int(2))},
		{value.NewList(value.Double(0), value.Int(-1)), value.NewList(value.Int(2), value.Double(3))},
		{value.NewList(), value.NewList(value.Int(2), value.Double(3))},
		{value.NewDict(map[string]value.Value{}), value.NewList(value.Int(2), value.Double(3))},
		{value.Nil, value.NewList(value.Int(2), value.Double(3))},
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
	h := NewAverage("somename", 123)
	actions := value.List{}
	expected := make([]Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 5; i++ {
		v := value.NewList(value.Int(i), value.String("hi"))
		d := value.NewDict(map[string]value.Value{
			"groupkey":  v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"value":     value.Int(i),
		})
		assert.NoError(t, actions.Append(d))
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_DAY, Index: 1, Width: 1, Value: value.NewList(value.Int(i), value.Int(1))})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_MINUTE, Index: uint64(24*10 + i*10), Width: 6, Value: value.NewList(value.Int(i), value.Int(1))})
	}
	buckets, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestRollingAverage_Bucketize_Invalid(t *testing.T) {
	t.Parallel()
	h := NewAverage("somename", 123)
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
		table := value.List{}
		for _, d := range test {
			assert.NoError(t, table.Append(d))
		}
		_, err := Bucketize(h, table)
		assert.Error(t, err, fmt.Sprintf("case was: %v", table))
	}
}

func TestRollingAverage_Start(t *testing.T) {
	h := average{Duration: 100}
	s, err := h.Start(110, value.Dict{})
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(10))
	// Duration > end
	s, err = h.Start(90, value.Dict{})
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(0))
	// Test kwargs
	s, err = h.Start(200, value.NewDict(map[string]value.Value{"duration": value.Int(50)}))
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(150))
}
