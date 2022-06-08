package counter

import (
	"math/rand"
	"sort"
	"testing"

	"fennel/lib/utils"

	"github.com/stretchr/testify/assert"

	"fennel/lib/value"
)

func TestTopK_Reduce(t *testing.T) {
	t.Parallel()
	h := NewTopK()
	numCases := 3
	cases := make([]struct {
		input  []value.Value
		output value.Value
	}, numCases)
	n := 1000
	keys := genKeys(n)

	cases[0].input = []value.Value{value.NewDict(map[string]value.Value{"abc": value.Double(5)})}
	cases[0].output = value.NewList(value.NewList(value.String("abc"), value.Int(5)))

	cases[1].input = []value.Value{}
	cases[1].output = value.NewList()

	cases[2].input = []value.Value{
		value.NewDict(map[string]value.Value{"a": value.Int(10), "b": value.Int(4), "c": value.Double(1.0)}),
		value.NewDict(map[string]value.Value{"a": value.Int(7), "b": value.Int(11), "c": value.Double(20.0)}),
		value.NewDict(map[string]value.Value{"a": value.Int(4), "b": value.Double(3.0), "c": value.Double(5.0)}),
	}
	cases[2].output = value.NewList(
		value.NewList(value.String("c"), value.Double(26)),
		value.NewList(value.String("a"), value.Double(21)),
		value.NewList(value.String("b"), value.Double(18)),
	)

	for i := 2; i < numCases; i++ {
		cases[i].input = []value.Value{}
		total := rand.Intn(100)
		for j := 0; j < total; j++ {
			cases[i].input = append(cases[i].input, genTopKDict(n, keys))
		}
		cases[i].output = findTopK(t, cases[i].input)
	}

	for _, c := range cases {
		found, err := h.Reduce(c.input)
		assert.NoError(t, err)
		assert.True(t, c.output.Equal(found))

		// and this works even when one of the elements is zero
		c.input = append(c.input, h.Zero())
		found, err = h.Reduce(c.input)
		assert.NoError(t, err)
		assert.True(t, c.output.Equal(found))
	}
}

func TestTopK_Merge_Valid(t *testing.T) {
	t.Parallel()
	h := NewTopK()
	validCases := make([][]value.Value, 2)

	validCases[0] = append(validCases[0], value.NewDict(nil))
	validCases[0] = append(validCases[0], value.NewDict(nil))
	validCases[0] = append(validCases[0], value.NewDict(nil))

	validCases[1] = append(validCases[1], value.NewDict(map[string]value.Value{"x": value.Double(5.5), "y": value.Int(-8)}))
	validCases[1] = append(validCases[1], value.NewDict(map[string]value.Value{"x": value.Int(-1), "y": value.Double(19.7)}))
	validCases[1] = append(validCases[1], value.NewDict(map[string]value.Value{"x": value.Double(4.5), "y": value.Double(11.7)}))

	for _, c := range validCases {
		found, err := h.Merge(c[0], c[1])
		assert.NoError(t, err)
		assert.Equal(t, c[2], found)
	}
}

func TestTopK_Merge_Invalid(t *testing.T) {
	t.Parallel()
	h := NewTopK()
	keys := genKeys(1000)
	validTopKVals := []value.Value{
		genTopKDict(0, nil),
		genTopKDict(1, keys[:1]),
		genTopKDict(2, keys[:2]),
		genTopKDict(3, keys[:3]),
		genTopKDict(1000, keys),
	}
	invalidTopKVals := []value.Value{
		value.Nil,
		value.NewList(value.Double(2)),
		value.String("xyz"),
		value.NewDict(map[string]value.Value{"x": value.Bool(false)}),
		value.NewDict(map[string]value.Value{"y": value.NewList(value.String("xyz"))}),
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
		_, err := h.Merge(nv, nv)
		assert.Error(t, err)
	}
}

func genTopKDict(n int, keys []string) value.Dict {
	d := value.NewDict(nil)
	for i := 0; i < n; i++ {
		if i%2 == 0 {
			d.Set(keys[i], value.Double(1e6*rand.Float64()))
		} else {
			d.Set(keys[i], value.Int(rand.Intn(1e6)))
		}
	}
	return d
}

func findTopK(t *testing.T, vals []value.Value) value.Value {
	dictVals := make([]value.Dict, len(vals))
	for i, v := range vals {
		dictVals[i] = v.(value.Dict)
	}
	d, err := topK{}.merge(dictVals)
	assert.NoError(t, err)
	type elem struct {
		key string
		val float64
	}
	l := make([]elem, 0, len(d.Iter()))
	for k, v := range d.Iter() {
		var v_ float64
		switch v := v.(type) {
		case value.Int:
			v_ = float64(v)
		case value.Double:
			v_ = float64(v)
		default:
			assert.Fail(t, "expected value to be int/float")
		}
		l = append(l, elem{k, v_})
	}
	sort.SliceStable(l, func(i, j int) bool {
		return l[i].val > l[j].val
	})
	var ret []value.Value
	for i := 0; i < numK && i < len(l); i++ {
		ret = append(ret, value.NewList(value.String(l[i].key), value.Double(l[i].val)))
	}
	return value.NewList(ret...)
}

func genKeys(n int) []string {
	var keys []string
	for i := 0; i < n; i++ {
		keys = append(keys, utils.RandString(32))
	}
	return keys
}
