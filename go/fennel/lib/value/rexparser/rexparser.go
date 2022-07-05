package rexparser

import (
	"errors"
	"fennel/lib/utils/binary"
	lib "github.com/buger/jsonparser"
)

// Primitive types
const BOOL = 0x00
const INT = 0xC0
const FLOAT = 0x80
const NULL = 0x40

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

// Find position of next character which is not whitespace
func nextToken(data []byte) int {
	for i, c := range data {
		switch c {
		case ' ', '\n', '\r', '\t':
			continue
		default:
			return i
		}
	}

	return -1
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

// Data types available in valid JSON data.
type ValueType int

const (
	NotExist = ValueType(iota)
	String
	Integer
	Float
	Object
	Array
	Tuple
	Boolean
	Null
	Unknown
)

func (vt ValueType) String() string {
	switch vt {
	case NotExist:
		return "non-existent"
	case Integer:
		return "integer"
	case Float:
		return "float"
	case String:
		return "string"
	case Object:
		return "object"
	case Array:
		return "array"
	case Tuple:
		return "tuple"
	case Boolean:
		return "boolean"
	case Null:
		return "null"
	default:
		return "unknown"
	}
}

var (
	trueLiteral  = []byte("true")
	falseLiteral = []byte("false")
	nullLiteral  = []byte("null")
)

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

func getType(data []byte, offset int) ([]byte, ValueType, int, int, error) {
	var dataType ValueType
	endOffset := offset
	// if string value
	if data[offset] == '"' {
		dataType = String
		if idx, _ := stringEnd(data[offset+1:]); idx != -1 {
			endOffset += idx + 1
		} else {
			return nil, dataType, offset, 0, MalformedStringError
		}
	} else if data[offset] == '[' { // if array value
		dataType = Array
		arrLength, endOffset, metadataOffset, err := getMetadata(data, offset)
		if err != nil {
			return nil, dataType, offset, 0, err
		}
		offset += metadataOffset
		endOffset += offset
		data[offset] = '['
		return data[offset:endOffset], dataType, endOffset, arrLength, nil
	} else if data[offset] == '{' {
		dataType = Object
		arrLength, endOffset, metadataOffset, err := getMetadata(data, offset)
		if err != nil {
			return nil, dataType, offset, 0, err
		}
		offset += metadataOffset
		endOffset += offset
		data[offset] = '{'
		return data[offset:endOffset], dataType, endOffset, arrLength, nil
	} else {
		// Number, Boolean or None
		end := tokenEnd(data[endOffset:])
		if end == -1 {
			return nil, dataType, offset, 0, MalformedValueError
		}

		// Extract first 2 bits to determine value type
		switch data[offset] & 0xC0 {
		case BOOL: // boolean
			dataType = Boolean
		case INT: // number
			dataType = Integer
		case FLOAT: // undefined or null
			dataType = Float
		case NULL:
			dataType = Null
		default:
			return nil, Unknown, offset, 0, UnknownValueTypeError
		}

		endOffset += end
	}
	return data[offset:endOffset], dataType, endOffset, 0, nil
}

/*
get - receives data structure, and key path to extract value from.

returns:
`value` - pointer to original data structure containing key value, or just empty slice if nothing found or error
`dataType` -    can be: `notexist`, `string`, `number`, `object`, `array`, `boolean` or `null`
`offset` - offset from provided data structure where key value ends. used mostly internally, for example for `arrayeach` helper.
`err` - if key not found or any other parsing issue it should return error. if key not found it also sets `dataType` to `notexist`

accept multiple keys to specify path to json value (in case of quering nested structures).
if no keys provided it will try to extract closest json value (simple ones or object/array), useful for reading streams or arrays, see `arrayeach` implementation.
*/
func Get(data []byte, keys ...string) (value []byte, dataType ValueType, offset, sz int, err error) {
	a, b, _, d, sz, e := internalGet(data)
	return a, b, d, sz, e
}

func internalGet(data []byte) (value []byte, dataType ValueType, offset, endOffset, sz int, err error) {

	// go to closest value
	nO := nextToken(data[offset:])
	if nO == -1 {
		return nil, NotExist, offset, -1, 0, MalformedJsonError
	}

	offset += nO
	value, dataType, endOffset, sz, err = getType(data, offset)
	if err != nil {
		return value, dataType, offset, endOffset, 0, err
	}

	// Strip quotes from string values
	if dataType == String {
		value = value[1 : len(value)-1]
	}

	return value[:len(value):len(value)], dataType, offset, endOffset, sz, nil
}

// ArrayEach is used when iterating arrays, accepts a callback function with the same return arguments as `Get`.
func ArrayEach(data []byte, cb func(value []byte, dataType ValueType, offset, sz int, err error), keys ...string) (offset int, err error) {
	if len(data) == 0 {
		return -1, MalformedObjectError
	}
	nT := nextToken(data)
	if nT == -1 {
		return -1, MalformedJsonError
	}

	offset = nT + 1
	nO := nextToken(data[offset:])
	if nO == -1 {
		return offset, MalformedJsonError
	}

	offset += nO

	if data[offset] == ']' {
		return offset, nil
	}

	for true {
		v, t, o, sz, e := Get(data[offset:])
		if e != nil {
			return offset, e
		}

		if o == 0 {
			break
		}

		if t != NotExist {
			cb(v, t, offset+o-len(v), sz, e)
		}

		if e != nil {
			break
		}

		offset += o

		skipToToken := nextToken(data[offset:])
		if skipToToken == -1 {
			return offset, MalformedArrayError
		}
		offset += skipToToken
		if data[offset] == ']' {
			break
		}

		if data[offset] != ',' {
			return offset, MalformedArrayError
		}

		offset++
	}

	return offset, nil
}

// ObjectEach iterates over the key-value pairs of a JSON object, invoking a given callback for each such entry
func ObjectEach(data []byte, callback func(key []byte, value []byte, dataType ValueType, offset, sz int) error, keys ...string) (err error) {
	offset := 0

	// Validate and skip past opening brace
	if off := nextToken(data[offset:]); off == -1 {
		return MalformedObjectError
	} else if offset += off; data[offset] != '{' {
		return MalformedObjectError
	} else {
		offset++
	}

	// Skip to the first token inside the object, or stop if we find the ending brace
	if off := nextToken(data[offset:]); off == -1 {
		return MalformedJsonError
	} else if offset += off; data[offset] == '}' {
		return nil
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
			return nil // we found the end of the object; stop and return success
		default:
			return MalformedObjectError
		}

		// Find the end of the key string
		var keyEscaped bool
		if off, esc := stringEnd(data[offset:]); off == -1 {
			return MalformedJsonError
		} else {
			key, keyEscaped = data[offset:offset+off-1], esc
			offset += off
		}

		// Unescape the string if needed
		if keyEscaped {
			var stackbuf [unescapeStackBufSize]byte // stack-allocated array for allocation-free unescaping of small strings
			if keyUnescaped, err := lib.Unescape(key, stackbuf[:]); err != nil {
				return MalformedStringEscapeError
			} else {
				key = keyUnescaped
			}
		}

		// Step 2: skip the colon
		if off := nextToken(data[offset:]); off == -1 {
			return MalformedJsonError
		} else if offset += off; data[offset] != ':' {
			return MalformedJsonError
		} else {
			offset++
		}

		// Step 3: find the associated value, then invoke the callback
		if value, valueType, off, sz, err := Get(data[offset:]); err != nil {
			return err
		} else if err := callback(key, value, valueType, offset+off, sz); err != nil { // Invoke the callback here!
			return err
		} else {
			offset += off
		}

		// Step 4: skip over the next comma to the following token, or stop if we hit the ending brace
		if off := nextToken(data[offset:]); off == -1 {
			return MalformedArrayError
		} else {
			offset += off
			switch data[offset] {
			case '}':
				return nil // Stop if we hit the close brace
			case ',':
				offset++ // Ignore the comma
			default:
				return MalformedObjectError
			}
		}

		// Skip to the next token after the comma
		if off := nextToken(data[offset:]); off == -1 {
			return MalformedArrayError
		} else {
			offset += off
		}
	}

	return MalformedObjectError // we shouldn't get here; it's expected that we will return via finding the ending brace
}

// ParseString parses a String ValueType into a Go string (the main parsing work is unescaping the JSON string)
func ParseString(b []byte) (string, error) {
	var stackbuf [unescapeStackBufSize]byte // stack-allocated array for allocation-free unescaping of small strings
	if bU, err := lib.Unescape(b, stackbuf[:]); err != nil {
		return "", MalformedValueError
	} else {
		return string(bU), nil
	}
}
