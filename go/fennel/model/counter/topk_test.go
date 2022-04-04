package counter

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func TestTopK_Reduce(t *testing.T) {
	t.Parallel()
	h := NewTopK("somename", []uint64{123})
	numCases := 7
	cases := make([]struct {
		input  []value.Value
		output value.Value
	}, numCases)

	cases[0].input = []value.Value{value.NewList(value.NewDict(map[string]value.Value{"data": value.Int(2), "score": value.Double(5)}))}
	cases[0].output = cases[0].input[0]

	cases[1].input = []value.Value{}
	cases[1].output = value.NewList()

	for i := 2; i < numCases; i++ {
		cases[i].input = []value.Value{}
		n := rand.Intn(100)
		for j := 0; j < n; j++ {
			cases[i].input = append(cases[i].input, genTopKList(numK))
		}
		cases[i].output = findTopK(cases[i].input)
	}

	for _, c := range cases {
		found, err := h.Reduce(c.input)
		assert.NoError(t, err)
		assert.Equal(t, c.output, found)

		// and this works even when one of the elements is zero
		c.input = append(c.input, h.Zero())
		assert.NoError(t, err)
		assert.Equal(t, c.output, found)
	}
}

func TestTopK_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := NewTopK("somename", []uint64{123})
	numCases := 7
	validCases := make([][]value.Value, numCases)

	validCases[0] = append(validCases[0], value.NewList())
	validCases[0] = append(validCases[0], value.NewList())
	validCases[0] = append(validCases[0], value.NewList())

	validCases[1] = append(validCases[1], value.NewList(value.NewDict(map[string]value.Value{"data": value.String("x"), "score": value.Double(-1)})))
	validCases[1] = append(validCases[1], value.NewList(value.NewDict(map[string]value.Value{"data": value.Double(3.1), "score": value.Double(19)})))
	validCases[1] = append(validCases[1], findTopK([]value.Value{validCases[1][0], validCases[1][1]}))

	for i := 2; i < numCases; i++ {
		a := genTopKList(numK)
		b := genTopKList(numK)
		validCases[i] = append(validCases[i], a)
		validCases[i] = append(validCases[i], b)
		validCases[i] = append(validCases[i], findTopK([]value.Value{a, b}))
	}
	for _, c := range validCases {
		found, err := h.Merge(c[0], c[1])
		assert.NoError(t, err)
		assert.Equal(t, c[2], found)
	}
}

func TestTopK_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := NewTopK("somename", []uint64{123})
	validTopKVals := []value.Value{
		genTopKList(0),
		genTopKList(1),
		genTopKList(2),
		genTopKList(3),
		genTopKList(100),
	}
	invalidTopKVals := []value.Value{
		value.NewDict(map[string]value.Value{}),
		value.Nil,
		value.NewList(value.Double(2)),
		value.NewList(value.NewDict(map[string]value.Value{})),
		value.NewList(value.NewDict(map[string]value.Value{"data": value.Int(0)})),
		value.NewList(value.NewDict(map[string]value.Value{"score": value.Double(0.0)})),
		value.NewList(value.NewDict(map[string]value.Value{"data": value.Int(0), "score": value.Int(0)})),
		value.NewList(value.NewDict(map[string]value.Value{"score": value.Double(0.0)}), value.NewDict(map[string]value.Value{"data": value.Int(0), "score": value.Int(0)})),
	}
	var allTopKVals []value.Value
	allTopKVals = append(allTopKVals, validTopKVals...)
	allTopKVals = append(allTopKVals, invalidTopKVals...)
	for _, nv := range invalidTopKVals {
		for _, v := range allTopKVals {
			_, err := h.Merge(v, nv)
			assert.Error(t, err)

			_, err = h.Merge(nv, v)
			assert.Error(t, err)
		}
	}
}

func TestTopK_Bucketize_Valid(t *testing.T) {
	t.Parallel()
	h := NewTopK("somename", []uint64{123})
	actions := value.NewList()
	expected := make([]Bucket, 0)
	DAY := 3600 * 24
	for i := 0; i < 5; i++ {
		v := value.NewList(value.Int(i), value.String("hi"))
		d := value.NewDict(map[string]value.Value{
			"groupkey":  v,
			"timestamp": value.Int(DAY + i*3600 + 1),
			"value":     value.NewDict(map[string]value.Value{"data": value.Int(i), "score": value.Int(i)}),
		})
		actions.Append(d)
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_DAY,
			Index: 1, Width: 1, Value: value.NewList(value.NewDict(map[string]value.Value{"data": value.Int(i), "score": value.Double(i)}))})
		expected = append(expected, Bucket{Key: v.String(), Window: ftypes.Window_MINUTE,
			Index: uint64(24*10 + i*10), Width: 6, Value: value.NewList(value.NewDict(map[string]value.Value{"data": value.Int(i), "score": value.Double(i)}))})
	}
	buckets, err := Bucketize(h, actions)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, buckets)
}

func TestTopK_Bucketize_Invalid(t *testing.T) {
	t.Parallel()
	h := NewMax("somename", []uint64{123})
	cases := [][]value.Dict{
		{value.NewDict(map[string]value.Value{})},
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

func genTopKList(n int) value.Value {
	l := make([]value.Value, n)
	for i := range l {
		l[i] = value.NewDict(map[string]value.Value{
			"data":  value.Nil,
			"score": value.Double(rand.NormFloat64()),
		})
	}
	sort.SliceStable(l, func(i, j int) bool {
		s, _ := l[i].(value.Dict).Get("score")
		a := float64(s.(value.Double))
		s, _ = l[j].(value.Dict).Get("score")
		b := float64(s.(value.Double))
		return a > b
	})
	return value.NewList(l...)
}

func findTopK(vals []value.Value) value.Value {
	var all []value.Value
	for _, v := range vals {
		v := v.(value.List)
		for i := 0; i < v.Len(); i++ {
			e, _ := v.At(i)
			all = append(all, e)
		}
	}
	sort.SliceStable(all, func(i, j int) bool {
		s, _ := all[i].(value.Dict).Get("score")
		a := float64(s.(value.Double))
		//a := float64(all[i].(value.Dict)["score"].(value.Double))
		s, _ = all[j].(value.Dict).Get("score")
		b := float64(s.(value.Double))
		//b := float64(all[j].(value.Dict)["score"].(value.Double))
		return a > b
	})
	if len(all) > numK {
		return value.NewList(all[:numK]...)
	}
	return value.NewList(all...)
}
