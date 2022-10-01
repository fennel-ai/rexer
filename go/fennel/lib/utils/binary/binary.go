package binary

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"

	"github.com/dennwc/varint"
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
	if sz == 0 {
		return 0, 0, fmt.Errorf("invalid uvarint: buffer (size: %d) too small", len(b))
	} else if sz < 0 {
		return 0, 0, fmt.Errorf("invalid uvarint: value too large (read %d bytes)", -sz)
	}
	return n, sz, nil
}

func ReadUvarintUnrolled(b []byte) (uint64, int, error) {
	val, n := varint.Uvarint(b)
	if n == 0 {
		return 0, 0, fmt.Errorf("invalid uvarint: %v", b)
	} else if n < 0 {
		return 0, 0, fmt.Errorf("value overflow: %v", b)
	}
	return val, n, nil
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

func PutUint16(b []byte, n uint16) (int, error) {
	if len(b) < 2 {
		return 0, errors.New("buffer too small for putting uint32")
	}
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, n)
	return copy(b, buf), nil
}

func ReadUint16(b []byte) (uint16, int, error) {
	if len(b) < 2 {
		return 0, 0, errors.New("buffer too small")
	}
	return binary.LittleEndian.Uint16(b[:2]), 2, nil
}

func ReadUint32(b []byte) (uint32, int, error) {
	if len(b) < 4 {
		return 0, 0, errors.New("buffer too small")
	}
	return binary.LittleEndian.Uint32(b[:4]), 4, nil
}

func ReadUint64(b []byte) (uint64, int, error) {
	if len(b) < 8 {
		return 0, 0, errors.New("buffer too small")
	}
	return binary.LittleEndian.Uint64(b[:8]), 8, nil
}

func PutUint64(b []byte, n uint64) (int, error) {
	if len(b) < 8 {
		return 0, errors.New("buffer too small for putting uint32")
	}
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, n)
	return copy(b, buf), nil
}

func PutUint32(b []byte, n uint32) (int, error) {
	if len(b) < 4 {
		return 0, errors.New("buffer too small for putting uint32")
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, n)
	return copy(b, buf), nil
}

func reverseArray(arr []byte) []byte {
	for i, j := 0, len(arr)-1; i < j; i, j = i+1, j-1 {
		arr[i], arr[j] = arr[j], arr[i]
	}
	return arr
}

func setMSB(data []byte) {
	if len(data) > 1 {
		data[0] = data[0] | 0x10
	}
	for i := 0; i < len(data); i++ {
		if i > 0 && i != len(data)-1 {
			data[i] = data[i] | 0x80
		}
	}
}

// The first 3 bits are for type.
// The remaining bits are encoded using 7 bits per byte.
func Num2Bytes(num int64) ([]byte, error) {
	tmpBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(tmpBuf, uint64(num))
	var buf []byte
	for i := 0; i < len(tmpBuf); i++ {
		if tmpBuf[i] != 0 {
			buf = tmpBuf[i:]
			break
		}
	}
	carry := uint8(0)
	ret := make([]byte, 0, len(buf)+1)
	// Number of bits in carry.
	numShift := 0
	for i := len(buf) - 1; i >= 0; i-- {
		// temp is formed by picking all bits from carry and LSB (7 - numShift) #bits in buf[i]
		temp := ((buf[i] << numShift) | carry) & 0x7f
		carry = buf[i] >> (7 - numShift)
		ret = append(ret, temp)
		numShift += 1
	}
	if carry != 0 {
		ret = append(ret, carry)
	}
	ret = reverseArray(ret)
	// If number fits with the type being encoded then we don't need additional byte for type.
	// This can fit numbers <= 16 within 1 byte.
	if len(ret) > 0 && (ret[0]&0xF0) == 0 {
		setMSB(ret)
		return ret, nil
	}
	ret = append([]byte{0}, ret...)
	setMSB(ret)
	return ret, nil
}

// ParseInteger returns the number encoded and the number of bytes taken.
func ParseInteger(data []byte) (int64, int, error) {
	var v int64
	if len(data) == 0 {
		return 0, 0, fmt.Errorf("integer data is empty")
	}
	v = int64(data[0] & 0xf)
	i := 0
	if data[0]&0x10 != 0 {
		i++
		for i < len(data) && data[i] >= 0x80 {
			v = v<<7 | int64(data[i]&0x7f)
			i++
		}
		if i >= len(data) {
			return 0, 0, fmt.Errorf("invalid integer")
		}
		v = v<<7 | int64(data[i]&0x7f)
	}
	return v, i + 1, nil
}
