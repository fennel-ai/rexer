package value

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func verifyMarshalUnMarshal(t *testing.T, v Value) {
	b, err := Marshal(v)
	assert.NoError(t, err)
	var u Value
	err = Unmarshal(b, &u)
	assert.NoError(t, err)
	assert.Equal(t, v, u)
}

func verifyUnequalMarshal(t *testing.T, v Value, unequal []Value) {
	b, err := Marshal(v)
	assert.NoError(t, err)
	var u Value
	err = Unmarshal(b, &u)
	for _, other := range unequal {
		assert.NotEqual(t, u, other)
	}
}

func TestEqualMarshal(t *testing.T) {
	verifyMarshalUnMarshal(t, Int(0))
	verifyMarshalUnMarshal(t, Int(120))
	verifyMarshalUnMarshal(t, Int(100000000))
	verifyMarshalUnMarshal(t, Int(141414141))
	verifyMarshalUnMarshal(t, Int(-411414141))
	verifyMarshalUnMarshal(t, Double(0.1))
	verifyMarshalUnMarshal(t, Double(-0.1))
	verifyMarshalUnMarshal(t, Double(0))
	verifyMarshalUnMarshal(t, Double(1e-4))
	verifyMarshalUnMarshal(t, Double(1e14))
	verifyMarshalUnMarshal(t, Bool(true))
	verifyMarshalUnMarshal(t, Bool(false))
	verifyMarshalUnMarshal(t, String(""))
	verifyMarshalUnMarshal(t, String("hi"))
	verifyMarshalUnMarshal(t, String("i12_%2342]{"))
	values := []Value{Int(1), Int(2), String("here"), Bool(false), Nil}
	list := NewList(values...)
	verifyMarshalUnMarshal(t, list)
	verifyMarshalUnMarshal(t, NewList())

	verifyMarshalUnMarshal(t, Nil)

	kwargs := make(map[string]Value, 0)
	kwargs["a"] = Int(1)
	kwargs["b"] = String("hi")
	kwargs["c"] = list
	dict1 := NewDict(kwargs)
	verifyMarshalUnMarshal(t, dict1)
	verifyMarshalUnMarshal(t, NewDict(map[string]Value{}))
}

func TestUnequalMarshal(t *testing.T) {
	i := Int(2)
	d := Double(3.0)
	b := Bool(false)
	s := String("hi")
	l := NewList(Int(1), Double(2.0), Bool(true))
	di := NewDict(map[string]Value{"a": Int(2), "b": Double(1.0)})
	n := Nil
	verifyUnequalMarshal(t, Int(123), []Value{i, d, b, s, l, di, n})
	verifyUnequalMarshal(t, Double(-5.0), []Value{i, d, b, s, l, di, n})
	verifyUnequalMarshal(t, String("bye"), []Value{i, d, b, s, l, di, n})
	verifyUnequalMarshal(t, Bool(true), []Value{i, d, b, s, l, di, n})
	verifyUnequalMarshal(t, Nil, []Value{i, d, b, s, l, di})
	verifyUnequalMarshal(t, NewList(Int(2), Bool(true)), []Value{i, d, b, s, l, di})
	verifyUnequalMarshal(t, NewDict(map[string]Value{"b": Int(2)}), []Value{i, d, b, s, l, di})
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func smallValue(n int) [][]byte {
	arr := make([][]byte, n)

	for i := 0; i < n; i++ {
		sample := NewList(String(RandStringRunes(5)), Int(rand.Int()), Bool(true))
		arr[i], _ = Marshal(sample)
	}
	return arr
}

func largeValue(n int) [][]byte {
	arr := make([][]byte, n)

	for i := 0; i < n; i++ {
		sample := NewDict(map[string]Value{})
		for j := 0; j < 200; j++ {
			l := NewList(Int(1), Double(2.0), Bool(true))
			for k := 0; k < 10; k++ {
				l.Append(String(RandStringRunes(5)))
			}
			sample.Set(RandStringRunes(5), l)
		}
		arr[i], _ = CaptainMarshal(sample)
	}
	return arr
}

func benchMarkAdityaProto(b *testing.B) {
	arr := smallValue(b.N)
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		var v Value
		err := Unmarshal(arr[n], &v)
		assert.NoError(b, err)
	}
}

func benchMarkAdityaCaptain(b *testing.B) {
	arr := largeValue(b.N)
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		var v Value
		err := CaptainUnmarshal(arr[n], &v)
		assert.NoError(b, err)
	}
}

func Benchmark_Marshal2(b *testing.B) {
	// benchMarkAdityaProto(b)
	benchMarkAdityaCaptain(b)
}
