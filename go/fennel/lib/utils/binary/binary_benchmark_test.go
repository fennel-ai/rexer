package binary

import (
	"encoding/binary"
	"testing"

	"github.com/dennwc/varint"
)

const maxUint64 = uint64(1<<64 - 1)

const (
	MaxVal9 = maxUint64 >> (1 + iota*7)
	MaxVal8
	MaxVal7
	MaxVal6
	MaxVal5
	MaxVal4
	MaxVal3
	MaxVal2
	MaxVal1
)


func benchmarkBinaryVarint(b *testing.B) {
	b.ReportAllocs()
	buf := make([]byte, binary.MaxVarintLen64)
	_ = binary.PutUvarint(buf[:], MaxVal1)

	var n int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, n = binary.Uvarint(buf[:])
	}
	if n != 1 {
		b.Fatal(n)
	}
}

func benchmarkVarint(b *testing.B) {
	b.ReportAllocs()
	buf := make([]byte, binary.MaxVarintLen64)
	_ = binary.PutUvarint(buf[:], MaxVal1)

	var n int
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, n = varint.Uvarint(buf)
	}
	if n != 1 {
		b.Fatal(n)
	}
}

func BenchmarkBinary(b *testing.B) {
	b.Run("binary.Uvarint", benchmarkBinaryVarint)
	b.Run("varint.Uvarint", benchmarkVarint)
}