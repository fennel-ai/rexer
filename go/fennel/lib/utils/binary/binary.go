package binary

import (
	"encoding/binary"
	"fmt"
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
