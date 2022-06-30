package value

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
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

var SZ = "l"

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func smallValue(n int, t string) ([][]byte, []Value) {
	arr := make([][]byte, n)
	samples := make([]Value, n)
	totalSize := 0
	for i := 0; i < n; i++ {
		sample := NewList(String(RandStringRunes(5)), Int(rand.Int()), Bool(true))
		samples[i] = sample
		if t == "captain" {
			arr[i], _ = CaptainMarshal(sample)
		} else if t == "proto" {
			arr[i], _ = Marshal(sample)
		} else if t == "json" {
			arr[i], _ = sample.MarshalJSON()
		} else {
			panic("unknown type")
		}
		totalSize += len(arr[i])
	}
	fmt.Println("Total size: ", totalSize/n)
	return arr, samples
}

func largeValue(n int, t string) ([][]byte, []Value) {
	arr := make([][]byte, n)
	samples := make([]Value, n)
	totalSize := 0
	for i := 0; i < n; i++ {
		sample := NewDict(map[string]Value{})
		for j := 0; j < 200; j++ {
			l := NewList(Int(1), Double(2.0), Bool(true))
			for k := 0; k < 10; k++ {
				l.Append(String(RandStringRunes(5)))
			}
			sample.Set(RandStringRunes(5), l)
		}
		if t == "captain" {
			arr[i], _ = CaptainMarshal(sample)
		} else if t == "proto" {
			arr[i], _ = Marshal(sample)
		} else if t == "json" {
			arr[i], _ = sample.MarshalJSON()
		} else {
			panic("unknown type")
		}
		totalSize += len(arr[i])
		samples[i] = sample
	}
	fmt.Printf("total size: %d\n", totalSize/n)
	return arr, samples
}

func benchMarkProtoSerialization(b *testing.B) {
	fmt.Println("Benchmarking Proto ", b.N)
	var arr [][]byte
	//var samples []Value
	if SZ == "l" {
		arr, _ = largeValue(b.N, "proto")
	} else {
		arr, _ = smallValue(b.N, "proto")
	}
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		var v Value
		err := Unmarshal(arr[n], &v)
		//assert.True(b, v.Equal(samples[n]))
		assert.NoError(b, err)
	}
}

func benchMarkCaptainSerialization(b *testing.B) {
	fmt.Println("Benchmarking Captain ", b.N)
	var arr [][]byte
	if SZ == "l" {
		arr, _ = largeValue(b.N, "captain")
	} else {
		arr, _ = smallValue(b.N, "captain")
	}
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, err := CaptainUnmarshal(arr[n])
		//assert.True(b, v.Equal(samples[n]))
		assert.NoError(b, err)
	}
}

func benchMarkJsonSerialization(b *testing.B) {
	fmt.Println("Benchmarking Json ", b.N)
	var arr [][]byte
	if SZ == "l" {
		arr, _ = largeValue(b.N, "json")
	} else {
		arr, _ = smallValue(b.N, "json")
	}
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, err := FromJSON(arr[n])
		//assert.True(b, v.Equal(samples[n]))
		assert.NoError(b, err)
	}
}

func Benchmark_Serialization(b *testing.B) {
	benchMarkProtoSerialization(b)
	//benchMarkCaptainSerialization(b)
	//benchMarkJsonSerialization(b)
}
