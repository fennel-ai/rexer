package counter

import (
	"log"
	"math/rand"
	"sort"
	"testing"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
)

func TestTopK_Reduce(t *testing.T) {
	t.Parallel()
	h := NewTopK("somename", 123)
	numCases := 7
	cases := make([]struct {
		input  []value.Value
		output value.Value
	}, numCases)

	cases[0].input = []value.Value{value.List{value.Dict{"data": value.Int(2), "score": value.Double(5)}}}
	cases[0].output = cases[0].input[0]

	cases[1].input = []value.Value{}
	cases[1].output = value.List{}

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
	h := NewTopK("somename", 123)
	numCases := 7
	validCases := make([][]value.Value, numCases)

	validCases[0] = append(validCases[0], value.List{})
	validCases[0] = append(validCases[0], value.List{})
	validCases[0] = append(validCases[0], value.List{})

	validCases[1] = append(validCases[1], value.List{value.Dict{"data": value.String("x"), "score": value.Double(-1)}})
	validCases[1] = append(validCases[1], value.List{value.Dict{"data": value.Double(3.1), "score": value.Double(19)}})
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
	h := NewTopK("somename", 123)
	validTopKVals := []value.Value{
		genTopKList(0),
		genTopKList(1),
		genTopKList(2),
		genTopKList(3),
		genTopKList(100),
	}
	invalidTopKVals := []value.Value{
		value.Dict{},
		value.Nil,
		value.List{value.Double(2)},
		value.List{value.Dict{}},
		value.List{value.Dict{"data": value.Int(0)}},
		value.List{value.Dict{"score": value.Double(0.0)}},
		value.List{value.Dict{"data": value.Int(0), "score": value.Int(0)}},
		value.List{
			value.Dict{"score": value.Double(0.0)},
			value.Dict{"data": value.Int(0), "score": value.Int(0)},
		},
	}
	var allTopKVals []value.Value
	allTopKVals = append(allTopKVals, validTopKVals...)
	allTopKVals = append(allTopKVals, invalidTopKVals...)
	for _, nv := range invalidTopKVals {
		for _, v := range allTopKVals {
			_, err := h.Merge(v, nv)
			if err == nil {
				log.Print("valid:", v)
				log.Print("invalid:", nv)
			}
			assert.Error(t, err)

			_, err = h.Merge(nv, v)
			assert.Error(t, err)
		}
	}
}

func TestTopK_Start(t *testing.T) {
	h := topK{Duration: 100}
	s, err := h.Start(110, value.Dict{})
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(10))
	// Duration > end
	s, err = h.Start(90, value.Dict{})
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(0))
	// Test kwargs
	s, err = h.Start(200, value.Dict{"duration": value.Int(50)})
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(150))
}

func genTopKList(n int) value.Value {
	l := make([]value.Value, n)
	for i := range l {
		l[i] = value.Dict{
			"data":  value.Nil,
			"score": value.Double(rand.NormFloat64()),
		}
	}
	sort.SliceStable(l, func(i, j int) bool {
		a := float64(l[i].(value.Dict)["score"].(value.Double))
		b := float64(l[j].(value.Dict)["score"].(value.Double))
		return a > b
	})
	return value.List(l)
}

func findTopK(vals []value.Value) value.Value {
	var all []value.Value
	for _, v := range vals {
		v := v.(value.List)
		all = append(all, v...)
	}
	sort.SliceStable(all, func(i, j int) bool {
		a := float64(all[i].(value.Dict)["score"].(value.Double))
		b := float64(all[j].(value.Dict)["score"].(value.Double))
		return a > b
	})
	if len(all) > numK {
		return value.List(all[:numK])
	}
	return value.List(all)
}
