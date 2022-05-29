package counter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
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

func TestList_Bucketize_Valid(t *testing.T) {
	t.Parallel()
	h := NewList([]uint64{123})
	actions := value.NewList()
	expected := make([]counter.Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 5; i++ {
		v := value.Int(1)
		e := value.Int(i)
		d := value.NewDict(map[string]value.Value{
			"groupkey":  v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"value":     e,
		})
		actions.Append(d)
		expected = append(expected, counter.Bucket{Value: value.NewList(e), Window: ftypes.Window_DAY, Index: 1, Width: 1, Key: v.String()})
		expected = append(expected, counter.Bucket{Key: v.String(), Window: ftypes.Window_MINUTE, Index: uint64(24*10 + i*10), Width: 6, Value: value.NewList(e)})
	}
	buckets, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestList_Bucketize_Invalid(t *testing.T) {
	t.Parallel()
	h := NewList([]uint64{123})
	cases := [][]*value.Dict{
		{value.NewDict(nil)},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "timestamp": value.Int(2)})},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "timestamp": value.Bool(true), "value": value.Int(4)})},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "timestamp": value.Double(1.0), "value": value.Int(3)})},
		{value.NewDict(map[string]value.Value{"groupkey": value.Int(1), "value": value.Int(3)})},
		{value.NewDict(map[string]value.Value{"timestamp": value.Int(1), "value": value.Int(3)})},
	}
	for _, test := range cases {
		table := value.NewList()
		for _, d := range test {
			table.Append(d)
		}
		_, err := Bucketize(h, table)
		assert.Error(t, err, fmt.Sprintf("case was: %v", table))
	}
}
