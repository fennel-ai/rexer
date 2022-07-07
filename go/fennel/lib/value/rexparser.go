package value

import (
	binlib "encoding/binary"
	"errors"
	"fennel/lib/utils/binary"
	"fmt"
	"math"
)

// Primitive types
// First 3 bits represent the type.
// New type can use 0xE0 or additional types can use 0x0 type.

const DICT = 0x20
const LIST = 0x40
const STRING = 0x60
const POS_INT = 0x80
const NEG_INT = 0xA0
const DOUBLE = 0xC0

const NULL = 0x00
const TRUE = 0x1
const FALSE = 0x2

const MAX_ALLOC_SIZE = 10000000

// Errors
var (
	EmptyValueError       = errors.New("serialized bytes are empty")
	MalformedDictError    = errors.New("malformed dictionary serialization")
	MalformedDictKeyError = errors.New("malformed dictionary key serialization")
	MalformedStringError  = errors.New("malformed string serialization")
	MalformedListError    = errors.New("malformed list serialization")
	MalformedDoubleError  = errors.New("insufficient bytes for double")
)

func EncodeTypeWithNum(t byte, n int64) ([]byte, error) {
	ret, err := binary.Num2Bytes(n)
	if err != nil {
		return nil, err
	}
	if len(ret) == 0 {
		return nil, fmt.Errorf("invalid number passed to EncodeTypeWithNum")
	}
	ret[0] = t | ret[0]
	return ret, nil
}

// ParseValue returns the value and the amount of bytes consumed by the value.
func ParseValue(data []byte) (Value, int, error) {
	if len(data) == 0 {
		return nil, 0, EmptyValueError
	}
	switch data[0] & 0xE0 {
	case STRING:
		length, offset, err := binary.ParseInteger(data)
		if err != nil {
			return nil, 0, err
		}
		if length < 0 || len(data) < offset+int(length) {
			return nil, 0, MalformedStringError
		}
		s := string(data[offset : offset+int(length)])
		return String(s), offset + int(length), nil
	case LIST:
		arrLength, offset, err := binary.ParseInteger(data)
		if err != nil {
			return nil, 0, err
		}
		if len(data) < offset {
			return nil, 0, MalformedListError
		}
		return parseArray(data[offset:], offset, int(arrLength))
	case DICT:
		dictLength, offset, err := binary.ParseInteger(data)
		if err != nil {
			return nil, 0, err
		}
		if len(data) < offset {
			return nil, 0, MalformedDictError
		}
		return parseDict(data[offset:], offset, int(dictLength))
	case POS_INT:
		n, offset, err := binary.ParseInteger(data)
		return Int(n), offset, err
	case NEG_INT:
		n, offset, err := binary.ParseInteger(data)
		return Int(-n), offset, err
	case DOUBLE:
		// Double takes 9 bytes, 1 for the type, 8 for the value
		if len(data) < 9 {
			return nil, 0, MalformedDoubleError
		}
		d := binlib.BigEndian.Uint64(data[1:9])
		f := math.Float64frombits(d)
		return Double(f), 9, nil
	default:
		if data[0] == NULL {
			return Nil, 1, nil
		}
		v, err := parseBoolean(data[0])
		return v, 1, err
	}
}

func parseBoolean(data byte) (Value, error) {
	if data == TRUE {
		return Bool(true), nil
	} else if data == FALSE {
		return Bool(false), nil
	} else {
		return nil, fmt.Errorf("invalid boolean value")
	}
}

func parseArray(data []byte, metadataSz, sz int) (Value, int, error) {
	if sz == 0 {
		return NewList(), metadataSz, nil
	}
	if sz > MAX_ALLOC_SIZE {
		return nil, 0, fmt.Errorf("array size is too large")
	}
	var ret List
	ret.Grow(sz)
	offset := 0
	for i := 0; i < sz; i++ {
		if len(data) < offset {
			return nil, 0, MalformedListError
		}
		v, o, e := ParseValue(data[offset:])
		if e != nil {
			return nil, 0, e
		}
		ret.Append(v)
		offset += o
	}
	return ret, offset + metadataSz, nil
}

func parseDict(data []byte, metadataSz, sz int) (Value, int, error) {
	if sz == 0 {
		return NewDict(map[string]Value{}), metadataSz, nil
	}
	if sz > MAX_ALLOC_SIZE {
		return nil, 0, fmt.Errorf("dict size is too large")
	}

	ret := make(map[string]Value, sz)
	offset := 0
	for i := 0; i < sz; i++ {
		if len(data) < offset {
			return nil, 0, MalformedDictError
		}
		// Step 1: find the next key
		length, o, err := binary.ParseInteger(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		offset += o
		if length < 0 || len(data) < offset+int(length) {
			return nil, 0, MalformedDictKeyError
		}
		key := string(data[offset : offset+int(length)])
		offset += int(length)
		if len(data) < offset {
			return nil, 0, MalformedDictError
		}
		// Step 2: find the associated value
		if v, o, e := ParseValue(data[offset:]); e != nil {
			return nil, 0, e
		} else {
			ret[key] = v
			offset += o
		}
	}
	return NewDict(ret), offset + metadataSz, nil
}
