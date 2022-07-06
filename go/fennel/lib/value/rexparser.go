package value

import (
	"errors"
	"fennel/lib/utils/binary"
	"fmt"
	lib "github.com/buger/jsonparser"
)

// Primitive types
const BOOL = 0x40
const INT = 0xC0
const FLOAT = 0x80
const NULL = 0x00
const TRUE = 0x01

// Errors
var (
	KeyPathNotFoundError       = errors.New("Key path not found")
	UnknownValueTypeError      = errors.New("Unknown value type")
	MalformedJsonError         = errors.New("Malformed JSON error")
	MalformedStringError       = errors.New("Value is string, but can't find closing '\"' symbol")
	MalformedArrayError        = errors.New("Value is array, but can't find closing ']' symbol")
	MalformedObjectError       = errors.New("Value looks like object, but can't find closing '}' symbol")
	MalformedValueError        = errors.New("Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol")
	OverflowIntegerError       = errors.New("Value is number, but overflowed while parsing")
	MalformedStringEscapeError = errors.New("Encountered an invalid escape sequence in a string")
)

// How much stack space to allocate for unescaping JSON strings; if a string longer
// than this needs to be escaped, it will result in a heap allocation
const unescapeStackBufSize = 64

func tokenEnd(data []byte) int {
	for i, c := range data {
		switch c {
		case ' ', '\n', '\r', '\t', ',', '}', ']':
			return i
		}
	}

	return len(data)
}

// Tries to find the end of string
// Support if string contains escaped quote symbols.
func stringEnd(data []byte) (int, bool) {
	escaped := false
	for i, c := range data {
		if c == '"' {
			if !escaped {
				return i + 1, false
			} else {
				j := i - 1
				for {
					if j < 0 || data[j] != '\\' {
						return i + 1, true // even number of backslashes
					}
					j--
					if j < 0 || data[j] != '\\' {
						break // odd number of backslashes
					}
					j--

				}
			}
		} else if c == '\\' {
			escaped = true
		}
	}

	return -1, escaped
}

func getMetadata(data []byte, offset int) (int, int, int, error) {
	arrLength, n1, err := binary.ReadUvarint(data[offset+1:])
	if err != nil {
		return 0, 0, 0, MalformedObjectError
	}
	endOffset, n2, err := binary.ReadUvarint(data[offset+n1+1:])
	if err != nil {
		return 0, 0, 0, MalformedObjectError
	}
	return int(arrLength), int(endOffset), n1 + n2, nil
}

// ParseValue returns the value and the current offset.
func ParseValue(data []byte, offset int) (Value, int, error) {
	endOffset := offset
	switch data[offset] {
	case '"':
		if idx, _ := stringEnd(data[offset+1:]); idx != -1 {
			endOffset += idx + 1
		} else {
			return nil, offset, MalformedStringError
		}
		s, err := parseString(data[offset+1 : endOffset-1])
		return String(s), endOffset, err
	case '[':
		arrLength, endOffset, metadataOffset, err := getMetadata(data, offset)
		if err != nil {
			return nil, 0, err
		}
		offset += metadataOffset
		endOffset += offset
		data[offset] = '['
		if endOffset > len(data) {
			return nil, 0, MalformedArrayError
		}
		v, err := parseArray(data[offset:endOffset], arrLength)
		return v, endOffset, err
	case '{':
		arrLength, endOffset, metadataOffset, err := getMetadata(data, offset)
		if err != nil {
			return nil, 0, err
		}
		offset += metadataOffset
		endOffset += offset
		data[offset] = '{'
		if endOffset > len(data) {
			return nil, 0, MalformedObjectError
		}
		v, err := parseDict(data[offset:endOffset], arrLength)
		return v, endOffset, err
	default:
		// Number, Boolean or None
		end := tokenEnd(data[endOffset:])
		if end == -1 {
			return nil, 0, MalformedValueError
		}
		endOffset += end
		// Extract first 2 bits to determine value type
		switch data[offset] & 0xC0 {
		case BOOL: // boolean
			v, err := parseBoolean(data[offset:endOffset])
			return v, endOffset, err
		case INT: // number
			return Int(binary.ParseInteger(data[offset:endOffset])), endOffset, nil
		case FLOAT:
			return Double(binary.ParseFloat(data[offset:endOffset])), endOffset, nil
		case NULL:
			return Nil, endOffset, nil
		default:
			return nil, 0, UnknownValueTypeError
		}
	}
}

func parseBoolean(data []byte) (Value, error) {
	if len(data) != 1 {
		return nil, fmt.Errorf("invalid boolean")
	}
	data[0] = data[0] & 0x3F
	if data[0] == 0 {
		return Bool(false), nil
	}
	return Bool(true), nil
}

// ArrayEach is used when iterating arrays, accepts a callback function with the same return arguments as `Get`.
func parseArray(data []byte, sz int) (Value, error) {
	if len(data) == 0 {
		return nil, MalformedObjectError
	}
	offset := 1

	if data[offset] == ']' {
		return NewList(), nil
	}

	var ret List
	ret.Grow(sz)

	for true {
		v, o, e := ParseValue(data[offset:], 0)
		if e != nil {
			return nil, e
		}
		ret.Append(v)

		if o == 0 {
			break
		}
		offset += o

		if data[offset] == ']' {
			break
		}

		if data[offset] != ',' {
			return nil, MalformedArrayError
		}
		offset++
	}
	return ret, nil
}

// ObjectEach iterates over the key-value pairs of a JSON object, invoking a given callback for each such entry
func parseDict(data []byte, sz int) (Value, error) {
	offset := 0

	// Validate and skip past opening brace
	if data[offset] != '{' {
		return nil, MalformedObjectError
	} else {
		offset++
	}

	ret := make(map[string]Value, sz)

	// Skip to the first token inside the object, or stop if we find the ending brace
	if data[offset] == '}' {
		return NewDict(ret), nil
	}

	// Loop pre-condition: data[offset] points to what should be either the next entry's key, or the closing brace (if it's anything else, the JSON is malformed)
	for offset < len(data) {
		// Step 1: find the next key
		var key []byte

		// Check what the the next token is: start of string, end of object, or something else (error)
		switch data[offset] {
		case '"':
			offset++ // accept as string and skip opening quote
		case '}':
			return NewDict(ret), nil // we found the end of the object; stop and return success
		default:
			return nil, MalformedObjectError
		}

		// Find the end of the key string
		var keyEscaped bool
		if off, esc := stringEnd(data[offset:]); off == -1 {
			return nil, MalformedJsonError
		} else {
			key, keyEscaped = data[offset:offset+off-1], esc
			offset += off
		}

		// Unescape the string if needed
		if keyEscaped {
			var stackbuf [unescapeStackBufSize]byte // stack-allocated array for allocation-free unescaping of small strings
			if keyUnescaped, err := lib.Unescape(key, stackbuf[:]); err != nil {
				return nil, MalformedStringEscapeError
			} else {
				key = keyUnescaped
			}
		}

		// Step 2: skip the colon
		if data[offset] != ':' {
			return nil, MalformedJsonError
		} else {
			offset++
		}

		// Step 3: find the associated value, then invoke the callback
		if v, o, e := ParseValue(data[offset:], 0); e != nil {
			return nil, e
		} else {
			k, err := parseString(key)
			if err != nil {
				return nil, err
			}
			ret[k] = v
			offset += o
		}

		// Step 4: skip over the next comma to the following token, or stop if we hit the ending brace
		switch data[offset] {
		case '}':
			return NewDict(ret), nil // Stop if we hit the close brace
		case ',':
			offset++ // Ignore the comma
		default:
			return nil, MalformedObjectError
		}
	}
	return nil, MalformedObjectError // we shouldn't get here; it's expected that we will return via finding the ending brace
}

// ParseString parses a String ValueType into a Go string (the main parsing work is unescaping the JSON string)
func parseString(b []byte) (string, error) {
	var stackbuf [unescapeStackBufSize]byte // stack-allocated array for allocation-free unescaping of small strings
	if bU, err := lib.Unescape(b, stackbuf[:]); err != nil {
		return "", MalformedValueError
	} else {
		return string(bU), nil
	}
}
