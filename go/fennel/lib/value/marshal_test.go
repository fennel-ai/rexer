package value

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func verifyMarshalUnMarshal(t *testing.T, v Value) {
	b, err := ProtoMarshal(v)
	assert.NoError(t, err)
	var u Value
	err = ProtoUnmarshal(b, &u)
	assert.NoError(t, err)
	assert.Equal(t, v, u)
}

func verifyUnequalMarshal(t *testing.T, v Value, unequal []Value) {
	b, err := ProtoMarshal(v)
	assert.NoError(t, err)
	var u Value
	err = ProtoUnmarshal(b, &u)
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

func RandStringRunes(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~!@#$%^&*()_+-=[]{}|;':,./<>?\"")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func randomValue(n int, t string) ([][]byte, []Value) {
	arr := make([][]byte, n)
	samples := make([]Value, n)
	totalSize := 0
	for i := 0; i < n; i++ {
		subSample := NewDict(map[string]Value{})
		for j := 0; j < 300; j++ {
			subSample.Set(RandStringRunes(5), NewList(String(RandStringRunes(5)), Int(rand.Int()), Bool(true), NewDict(map[string]Value{})))
			subSample.Set(RandStringRunes(5), String(RandStringRunes(5)))
			subSample.Set(RandStringRunes(5), Int(rand.Int()))
		}
		sample := NewList(Int(rand.Int()), Double(rand.Float64()), Bool(rand.Int()%2 == 0), subSample, Nil, NewList(), String(`";<>?/\|':,./`))
		samples[i] = sample
		switch t {
		case "captain":
			arr[i], _ = CaptainMarshal(sample)
		case "proto":
			arr[i], _ = ProtoMarshal(sample)
		case "json":
			arr[i], _ = sample.MarshalJSON()
		case "rexerjson":
			arr[i], _ = sample.Marshal()
		default:
			panic("unknown type")
		}
		totalSize += len(arr[i])
	}
	fmt.Println("Total size: ", totalSize/n)
	return arr, samples
}

func smallValue(n int, t string) ([][]byte, []Value) {
	arr := make([][]byte, n)
	samples := make([]Value, n)
	totalSize := 0
	for i := 0; i < n; i++ {
		sample := NewList(String(RandStringRunes(5)), Int(rand.Int()))
		samples[i] = sample
		switch t {
		case "captain":
			arr[i], _ = CaptainMarshal(sample)
		case "proto":
			arr[i], _ = ProtoMarshal(sample)
		case "json":
			arr[i], _ = sample.MarshalJSON()
		case "rexerjson":
			arr[i], _ = sample.Marshal()
		default:
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
			l := NewList(Double(rand.Float32()*100), Int(rand.Int63n(100)))
			for k := 0; k < 10; k++ {
				l.Append(String(RandStringRunes(5)))
			}
			//sample.Set(RandStringRunes(5), Int(rand.Int()))
			sample.Set(RandStringRunes(5), l)
		}
		switch t {
		case "captain":
			arr[i], _ = CaptainMarshal(sample)
		case "proto":
			arr[i], _ = ProtoMarshal(sample)
		case "json":
			arr[i], _ = sample.MarshalJSON()
		case "rexerjson":
			arr[i], _ = sample.Marshal()
		default:
			panic("unknown type")
		}
		totalSize += len(arr[i])
		samples[i] = sample
	}
	fmt.Printf("total size: %d\n", totalSize/n)
	return arr, samples
}

func benchMarkSerialization(b *testing.B, algo, sz string) {
	var arr [][]byte
	var samples []Value
	if sz == "l" {
		arr, samples = largeValue(b.N, algo)
	} else if sz == "s" {
		arr, samples = smallValue(b.N, algo)
	} else {
		arr, samples = randomValue(b.N, algo)
	}
	fmt.Println("Benchmarking Algo ", algo, len(samples))
	var v Value
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		fmt.Println("Bytes", algo, arr[n][0], arr[n][len(arr[n])-1])
		var err error
		switch algo {
		case "captain":
			_, err = CaptainUnmarshal(arr[n])
		case "proto":
			_ = ProtoUnmarshal(arr[n], &v)
		case "json":
			v, err = FromJSON(arr[n])
		case "rexerjson":
			v, err = Unmarshal(arr[n])
		}
		//assert.True(b, v.Equal(samples[n]))
		assert.NoError(b, err)
	}
}

// go test -tags dynamic  -bench Benchmark_Serialization -v fennel/lib/value -run ^$  -benchtime=10000x
// go test -tags dynamic  -bench Benchmark_Serialization -v fennel/lib/value -run ^$  -benchtime=60s
// go test -tags dynamic  -bench Benchmark_Serialization -v fennel/lib/value -run ^$  -benchtime=10000x -cpuprofile cpu.out
// go tool pprof -http=localhost:6060 cpu.out
func Benchmark_Serialization(b *testing.B) {
	benchMarkSerialization(b, "proto", "s")
	benchMarkSerialization(b, "rexerjson", "s")
}
