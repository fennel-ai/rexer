package value

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

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
	assert.NoError(t, err)
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
	kwargs["d"] = Double(1.2345)
	kwargs["e"] = Double(5.0)
	dict1 := NewDict(kwargs)
	verifyMarshalUnMarshal(t, dict1)
	verifyMarshalUnMarshal(t, NewDict(map[string]Value{}))
}

func TestRexparserMarshal(t *testing.T) {
	value := NewList(
		Int(1),
		Int(1024),
		String("happy"),
		Bool(false),
		NewDict(map[string]Value{"a": Int(2), "b": Double(-8964)}),
		Double(0),
		Double(0.61815),
		Double(-0.61815),
		Double(-0.125),
		Double(-3343),
	)
	data, err := value.Marshal()
	assert.NoError(t, err)
	value2, bytes, err := ParseValue(data)
	assert.NoError(t, err)
	assert.Equal(t, value2, value)
	assert.Equal(t, bytes, len(data))
	value0 := Double(0.00)
	data, _ = value0.Marshal()
	assert.Equal(t, len(data), 1) // 0.0 is encoded into 1 byte
	value2, bytes, _ = ParseValue(data)
	assert.Equal(t, value0, value2)
	assert.Equal(t, bytes, 1)
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

func createVal(seed, depth int) Value {
	if depth > 1 {
		return Nil
	}
	switch seed % 11 {
	case 0:
		return Int(rand.Int())
	case 1:
		return Int(-rand.Int())
	case 2:
		return Double(rand.Float64())
	case 3:
		return Double(-rand.Float64())
	case 4:
		return Bool(rand.Int()%2 == 0)
	case 5:
		return String(RandStringRunes(rand.Intn(20)))
	case 6:
		return Nil
	case 7:
		length := rand.Int() % 10000
		list := make([]Value, 0, length)
		for i := 0; i < length; i++ {
			list = append(list, createVal(rand.Int(), depth+1))
		}
		return NewList(list...)
	case 8:
		length := rand.Int() % 100000
		dict := make(map[string]Value, length)
		for i := 0; i < length; i++ {
			dict[RandStringRunes(rand.Intn(10))] = createVal(rand.Int(), depth+1)
		}
		return NewDict(dict)
	case 9:
		return NewList()
	case 10:
		return NewDict(map[string]Value{})
	}
	return Nil
}

func generateValue(n int, t string) ([][]byte, []Value) {
	arr := make([][]byte, n)
	samples := make([]Value, n)
	totalSize := 0
	fmt.Printf("Generating %d %s values\n", n, t)
	for i := 0; i < n; i++ {
		sample := createVal(rand.Int(), 0)
		samples[i] = sample
		switch t {
		case "proto":
			arr[i], _ = ProtoMarshal(sample)
		case "json":
			arr[i], _ = sample.MarshalJSON()
		case "rexparser":
			arr[i], _ = Marshal(sample)
		default:
			panic("unknown type")
		}
		totalSize += len(arr[i])
	}
	fmt.Println("Total siz", totalSize, "Number", n, "Avg size: ", totalSize/n)
	return arr, samples
}

func smallValue(n int, t string) ([][]byte, []Value) {
	arr := make([][]byte, n)
	samples := make([]Value, n)
	totalSize := 0
	for i := 0; i < n; i++ {
		//sample := NewList(String(RandStringRunes(5)), Int(rand.Int()))
		//x = `[5,0.9405090880450124]`
		//sample, _ := FromJSON([]byte(x))
		sample := createVal(rand.Int(), 0)
		samples[i] = sample
		switch t {
		case "proto":
			arr[i], _ = ProtoMarshal(sample)
		case "json":
			arr[i], _ = sample.MarshalJSON()
		case "rexparser":
			arr[i], _ = Marshal(sample)
		default:
			panic("unknown type")
		}
		totalSize += len(arr[i])
	}
	fmt.Println("Total size: ", totalSize/n)
	return arr, samples
}

func commonValue(n int, t string) ([][]byte, []Value) {
	arr := make([][]byte, n)
	samples := make([]Value, n)
	totalSize := 0
	for i := 0; i < n; i++ {
		sample := NewDict(map[string]Value{})
		for j := 0; j < 200; j++ {
			l := NewList(Double(rand.Float32()*100), Int(rand.Int63n(100)))
			sample.Set(RandStringRunes(5), l)
		}
		switch t {
		case "proto":
			arr[i], _ = ProtoMarshal(sample)
		case "json":
			arr[i], _ = sample.MarshalJSON()
		case "rexparser":
			arr[i], _ = Marshal(sample)
		default:
			panic("unknown type")
		}
		totalSize += len(arr[i])
		samples[i] = sample
	}
	fmt.Printf("total size: %d\n", totalSize/n)
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
			sample.Set(RandStringRunes(5), l)
		}
		switch t {
		case "proto":
			arr[i], _ = ProtoMarshal(sample)
		case "json":
			arr[i], _ = sample.MarshalJSON()
		case "rexparser":
			arr[i], _ = Marshal(sample)
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
	} else if sz == "r" {
		arr, samples = generateValue(b.N, algo)
	} else {
		arr, samples = commonValue(b.N, algo)
	}
	fmt.Println("Benchmarking Algo ", algo, len(samples))

	var v Value
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		var err error
		switch algo {
		case "proto":
			_ = ProtoUnmarshal(arr[n], &v)
		case "json":
			v, err = FromJSON(arr[n])
		case "rexparser":
			v, err = Unmarshal(arr[n])
		}
		assert.NoError(b, err)
		//assert.True(b, v.Equal(samples[n]))
	}
}

// go test -tags dynamic  -bench Benchmark_Serialization -v fennel/lib/value -run ^$
// go test -tags dynamic  -bench Benchmark_Serialization -v fennel/lib/value -run ^$  -benchtime=10000x -cpuprofile cpu.out
// go tool pprof -http=localhost:6060 cpu.out
func Benchmark_Serialization(b *testing.B) {
	benchMarkSerialization(b, "rexparser", "r")
}

func Benchmark_Small_Serialization(b *testing.B) {
	benchMarkSerialization(b, "rexparser", "s")
}

func FuzzRandom(f *testing.F) {
	f.Add([]byte{'{', '"', 'a', 'b', 'c', '"', ':', '1', '}'})
	f.Add([]byte{'{', '"', 'a', 'b', 'c', '"', ':', '1', '}', '{', '"', 'a', 'b', 'c', '"', ':', '1', '}'})
	f.Add([]byte{'[', ']'})
	f.Add([]byte{'{', '"', 'a', 'b', 'c', '"', ':', '1', '.', '2', '3', '}'})
	f.Add([]byte{'{', '"', 'a', 'b', 'c', '"', ':', '1', '}', '{', '"', 'a', 'b', 'c', '"', ':', '1', '}'})
	f.Add([]byte{'[', ']'})
	f.Add([]byte{'[', '2', '.', '4', '4', ']'})

	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := FromJSON(b)
		if err != nil {
			t.Skip()
		}
		jsonBytes, err := v.MarshalJSON()
		assert.NoError(t, err)
		_, err = Unmarshal(jsonBytes)
		if err != nil || string(jsonBytes) != string(b) {
			t.Skip()
		}
		protobufBytes, err := ProtoMarshal(v)
		assert.NoError(t, err)
		_, err = Unmarshal(protobufBytes)
		assert.NoError(t, err)
		vBytes, err := Marshal(v)
		assert.NoError(t, err)
		v2, err := Unmarshal(vBytes)
		assert.NoError(t, err)
		assert.True(t, v.Equal(v2))
	})
}

func FuzzRandomBytes(f *testing.F) {
	f.Add([]byte{REXER_CODEC_V1})
	f.Add([]byte{REXER_CODEC_V1, REXER_CODEC_V1})
	f.Add([]byte{REXER_CODEC_V1, 0x12})
	f.Add([]byte{REXER_CODEC_V1, 0x23})

	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = Unmarshal(b)
	})
}
