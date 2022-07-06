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
	EmptyValueError = errors.New("serialized bytes are empty")
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

// ParseValue returns the value and the current offset.
func ParseValue(data []byte) (Value, int, error) {
	if len(data) == 0 {
		return nil, 0, EmptyValueError
	}
	switch data[0] & 0xE0 {
	case STRING:
		length, offset := binary.ParseInteger(data)
		s := string(data[offset+1 : offset+1+int(length)])
		return String(s), offset + int(length), nil
	case LIST:
		arrLength, offset := binary.ParseInteger(data)
		return parseArray(data[offset+1:], int(arrLength))
	case DICT:
		arrLength, offset := binary.ParseInteger(data)
		return parseDict(data[offset+1:], int(arrLength))
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
		if data[0] == byte(0) {
			return Nil, 0, nil
		}
		v, err := parseBoolean(data[0])
		return v, 0, err
	}
}

func parseBoolean(data byte) (Value, error) {
	if data == TRUE {
		return Bool(true), nil
	}
	return Bool(false), nil
}

func parseArray(data []byte, sz int) (Value, int, error) {
	if sz == 0 {
		return NewList(), 0, nil
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
		offset++
	}
	return ret, offset, nil
}

func parseDict(data []byte, sz int) (Value, int, error) {
	ret := make(map[string]Value, sz)
	if sz == 0 {
		return NewDict(ret), 0, nil
	}

	offset := 0
	for i := 0; i < sz; i++ {
		// Step 1: find the next key
		length, o := binary.ParseInteger(data)
		offset += o
		key := string(data[offset+1 : offset+1+int(length)])
		offset += int(length) + 1
		// Step 2: find the associated value
		if v, o, e := ParseValue(data[offset:]); e != nil {
			return nil, 0, e
		} else {
			ret[key] = v
			offset += o
		}
		offset++
	}
	return NewDict(ret), offset + 1, nil
}
