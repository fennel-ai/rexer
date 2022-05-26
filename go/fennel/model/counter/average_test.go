package counter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func TestRollingAverage_Reduce(t *testing.T) {
	t.Parallel()
	h := NewAverage([]uint64{100})
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
	h := NewAverage([]uint64{100})
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
	h := NewAverage([]uint64{100})
	invalidCases := []struct {
		a value.Value
		b value.Value
	}{
		{value.NewList(value.Int(0), value.Int(-1), value.Int(4)), value.NewList(value.Int(2), value.Int(3))},
		{value.NewList(value.Int(0), value.Int(-1)), value.NewList(value.Int(2))},
		{value.NewList(value.Double(0), value.Int(-1)), value.NewList(value.Int(2), value.Double(3))},
		{value.NewList(), value.NewList(value.Int(2), value.Double(3))},
		{value.NewDict(nil), value.NewList(value.Int(2), value.Double(3))},
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
	h := NewAverage([]uint64{123})
	actions := value.List{}
	expected := make([]counter.Bucket, 0)
	for i := 0; i < 5; i++ {
		v := value.NewList(value.Int(i), value.String("hi"))
		d := value.NewDict(map[string]value.Value{
			"groupkey":  v,
			"timestamp": value.Int(i*360 + 50),
			"value":     value.Int(i),
		})
		actions.Append(d)
		expected = append(expected, counter.Bucket{
			Key:    v.String(),
			Window: ftypes.Window_FOREVER,
			Index:  uint64(i),
			Width:  360,
			Value:  value.NewList(value.Int(i), value.Int(1)),
		})
	}
	buckets, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestRollingAverage_Bucketize_Invalid(t *testing.T) {
	t.Parallel()
	h := NewAverage([]uint64{123})
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
			table.Append(d)
		}
		_, err := Bucketize(h, table)
		assert.Error(t, err, fmt.Sprintf("case was: %v", table))
	}
}
