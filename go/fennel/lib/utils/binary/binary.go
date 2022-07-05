package binary

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"unsafe"
)

func PutBytes(b []byte, in []byte) (int, error) {
	return PutString(b, *(*string)(unsafe.Pointer(&in)))
}

// ReadBytes doesn't allocate the underlying data, but only creates the slice header
func ReadBytes(b []byte) ([]byte, int, error) {
	len_, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, 0, fmt.Errorf("invalid string")
	}
	if len(b) < n+int(len_) {
		return nil, 0, fmt.Errorf("buffer too small")
	}
	return b[n : n+int(len_)], n + int(len_), nil
}

func PutString(b []byte, s string) (int, error) {
	len_ := len(s)
	lenbuf := [10]byte{}
	n := binary.PutUvarint(lenbuf[:], uint64(len_))
	if len(b) < n+len(s) {
		return 0, fmt.Errorf("buffer too small")
	}
	copy(b[:n], lenbuf[:n])
	copy(b[n:], s)
	return n + len_, nil
}

func ReadString(b []byte) (string, int, error) {
	bytes, n, err := ReadBytes(b)
	if err != nil {
		return "", n, err
	}
	return *(*string)(unsafe.Pointer(&bytes)), n, err
}

func PutUvarint(b []byte, n uint64) (int, error) {
	lenbuf := [10]byte{}
	sz := binary.PutUvarint(lenbuf[:], n)
	if len(b) < sz {
		return 0, fmt.Errorf("buffer too small")
	}
	copy(b, lenbuf[:sz])
	return sz, nil
}

func ReadUvarint(b []byte) (uint64, int, error) {
	n, sz := binary.Uvarint(b)
	if sz <= 0 {
		return 0, 0, fmt.Errorf("invalid uvarint")
	}
	return n, sz, nil
}

func ReadVarint(b []byte) (int64, int, error) {
	n, sz := binary.Varint(b)
	if sz <= 0 {
		return 0, 0, fmt.Errorf("invalid varint")
	}
	return n, sz, nil
}

func PutVarint(b []byte, n int64) (int, error) {
	lenbuf := [10]byte{}
	sz := binary.PutVarint(lenbuf[:], n)
	if len(b) < sz {
		return 0, fmt.Errorf("buffer too small")
	}
	copy(b, lenbuf[:sz])
	return sz, nil
}

func reverseArray(arr []byte) []byte {
	for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
		arr[i], arr[j] = arr[j], arr[i]
	}
	return arr
}

// The first 2 bits are for type, the 3rd bit for sign.
// The remaining bits are encoded using 7 bits per byte.
// The first bit is always 1 to distinguish from the ASCII characters.
func Num2Bytes[T int64 | float64](num T) ([]byte, error) {
	sign := uint8(0)
	if num < 0 {
		sign = 0x20
		num = -num
	}

	tmpBuf := make([]byte, 8)
	switch reflect.TypeOf(num).Kind() {
	case reflect.Int64:
		binary.BigEndian.PutUint64(tmpBuf, uint64(num))
	case reflect.Float64:
		binary.BigEndian.PutUint64(tmpBuf, math.Float64bits(float64(num)))
	default:
		return nil, fmt.Errorf("invalid type")
	}

	var buf []byte
	for i := 0; i < 8; i++ {
		if tmpBuf[i] != 0 {
			buf = tmpBuf[i:]
			break
		}
	}
	carry := uint8(0)
	var ret []byte
	// Number of bits in carry.
	numShift := 0
	for i := len(buf) - 1; i >= 0; i-- {
		// temp is formed by picking all bits from carry and LSB (7 - numShift) #bits in buf[i]
		temp := ((buf[i] << numShift) | carry) & 0x7f
		carry = buf[i] >> (7 - numShift)
		if i > 0 {
			// Set the first bit to 1 to distinguish from ASCII characters.
			// Only needed for the non type encoded bytes.
			temp = temp | 0x80
		}
		ret = append(ret, temp)
		numShift += 1
	}
	if carry != 0 {
		ret[len(ret)-1] = ret[len(ret)-1] | 0x80
		ret = append(ret, carry)
	}
	ret = reverseArray(ret)
	if len(ret) > 0 && (ret[0]&0xE0) == 0 {
		ret[0] = ret[0] | sign
		return ret, nil
	}
	if len(ret) > 1 {
		ret[0] = ret[0] | 0x80
	}
	ret = append([]byte{0}, ret...)
	ret[0] = ret[0] | sign
	return ret, nil
}

func parseInteger(data []byte) int64 {
	var v int64
	v = int64(data[0] & 0x1f)

	if len(data) > 1 {
		for i := 1; i < len(data); i++ {
			v = v<<7 | int64(data[i]&0x7f)
		}
	}
	if (data[0] & 0x20) == 0 {
		return v
	}
	return -v
}

func parseFloat(data []byte) float64 {
	var v uint64
	v = uint64(data[0] & 0x1f)

	if len(data) > 1 {
		for i := 1; i < len(data); i++ {
			v = v<<7 | uint64(data[i]&0x7f)
		}
	}
	d := math.Float64frombits(v)
	if (data[0] & 0x20) == 0 {
		return d
	}
	return -d
}
