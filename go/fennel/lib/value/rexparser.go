package value

import (
	"errors"
	"fennel/lib/utils/binary"
	"fmt"
)

// Primitive types
// First 3 bits represent the type.
// All additional types can use 0x0 type.

const DICT = 0x20
const LIST = 0x40
const STRING = 0x60
const POS_INT = 0x80
const NEG_INT = 0xA0
const POS_FLOAT = 0xC0
const NEG_FLOAT = 0xE0

const NULL = 0x00
const TRUE = 0x1
const FALSE = 0x2

// Errors
var (
	EmptyValueError    = errors.New("serialized bytes are empty")
	MalformedDictError = errors.New("malformed dictionary serialization")
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
		length, offset := binary.ParseInteger(data)
		s := string(data[offset : offset+int(length)])
		return String(s), offset + int(length), nil
	case LIST:
		arrLength, offset := binary.ParseInteger(data)
		return parseArray(data[offset:], offset, int(arrLength))
	case DICT:
		arrLength, offset := binary.ParseInteger(data)
		return parseDict(data[offset:], offset, int(arrLength))
	case POS_INT: // number
		n, offset := binary.ParseInteger(data)
		return Int(n), offset, nil
	case NEG_INT:
		n, offset := binary.ParseInteger(data)
		return Int(-n), offset, nil
	case POS_FLOAT:
		n, offset := binary.ParseFloat(data)
		return Double(n), offset, nil
	case NEG_FLOAT:
		n, offset := binary.ParseFloat(data)
		return Double(-n), offset, nil
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
	}
	return Bool(false), nil
}

func parseArray(data []byte, metadataSz, sz int) (Value, int, error) {
	if sz == 0 {
		return NewList(), metadataSz, nil
	}

	var ret List
	ret.Grow(sz)
	offset := 0
	for i := 0; i < sz; i++ {
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
	ret := make(map[string]Value, sz)
	if sz == 0 {
		return NewDict(ret), metadataSz, nil
	}

	offset := 0
	for i := 0; i < sz; i++ {
		// Step 1: find the next key
		length, o := binary.ParseInteger(data[offset:])
		offset += o
		if len(data) < offset+int(length) {
			return nil, 0, MalformedDictError
		}

		key := string(data[offset : offset+int(length)])
		offset += int(length)
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
