package rexparser

import (
	"bytes"
	"encoding/binary"
	"errors"
	lib "github.com/buger/jsonparser"
	"strconv"
)

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

func equalStr(b *[]byte, s string) bool {
	return string(*b) == s
}

func parseFloat(b *[]byte) (float64, error) {
	return strconv.ParseFloat(string(*b), 64)
}

func bytesToString(b *[]byte) string {
	return string(*b)
}

func StringToBytes(s string) []byte {
	return []byte(s)
}

const absMinInt64 = 1 << 63
const maxInt64 = 1<<63 - 1
const maxUint64 = 1<<64 - 1

// About 2x faster then strconv.ParseInt because it only supports base 10, which is enough for JSON
func parseInt(bytes []byte) (v int64, ok bool, overflow bool) {
	if len(bytes) == 0 {
		return 0, false, false
	}

	var neg bool = false
	if bytes[0] == '-' {
		neg = true
		bytes = bytes[1:]
	}

	var n uint64 = 0
	for _, c := range bytes {
		if c < '0' || c > '9' {
			return 0, false, false
		}
		if n > maxUint64/10 {
			return 0, false, true
		}
		n *= 10
		n1 := n + uint64(c-'0')
		if n1 < n {
			return 0, false, true
		}
		n = n1
	}

	if n > maxInt64 {
		if neg && n == absMinInt64 {
			return -absMinInt64, true, false
		}
		return 0, false, true
	}

	if neg {
		return -int64(n), true, false
	} else {
		return int64(n), true, false
	}
}

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

func findTokenStart(data []byte, token byte) int {
	for i := len(data) - 1; i >= 0; i-- {
		switch data[i] {
		case token:
			return i
		case '[', '{':
			return 0
		}
	}

	return 0
}

// Find position of next character which is not whitespace
func nextToken(data []byte) int {
	//return 0
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

// Find end of the data structure, array or object.
// For array openSym and closeSym will be '[' and ']', for object '{' and '}'
func blockEnd(data []byte, openSym byte, closeSym byte) int {
	level := 0
	i := 0
	ln := len(data)

	for i < ln {
		//fmt.Println("Finding end of block:", i)
		switch data[i] {
		case '"': // If inside string, skip it
			se, _ := stringEnd(data[i+1:])
			if se == -1 {
				return -1
			}
			i += se
		case openSym: // If open symbol, increase level
			level++
		case closeSym: // If close symbol, increase level
			level--

			// If we have returned to the original level, we're done
			if level == 0 {
				return i + 1
			}
		}
		i++
	}

	return -1
}

//
//func searchKeys(data []byte, keys ...string) int {
//	keyLevel := 0
//	level := 0
//	i := 0
//	ln := len(data)
//	lk := len(keys)
//	lastMatched := true
//
//	if lk == 0 {
//		return 0
//	}
//
//	var stackbuf [unescapeStackBufSize]byte // stack-allocated array for allocation-free unescaping of small strings
//
//	for i < ln {
//		switch data[i] {
//		case '"':
//			i++
//			keyBegin := i
//
//			strEnd, keyEscaped := stringEnd(data[i:])
//			if strEnd == -1 {
//				return -1
//			}
//			i += strEnd
//			keyEnd := i - 1
//
//			valueOffset := nextToken(data[i:])
//			if valueOffset == -1 {
//				return -1
//			}
//
//			i += valueOffset
//
//			// if string is a key
//			if data[i] == ':' {
//				if level < 1 {
//					return -1
//				}
//
//				key := data[keyBegin:keyEnd]
//
//				// for unescape: if there are no escape sequences, this is cheap; if there are, it is a
//				// bit more expensive, but causes no allocations unless len(key) > unescapeStackBufSize
//				var keyUnesc []byte
//				if !keyEscaped {
//					keyUnesc = key
//				} else if ku, err := lib.Unescape(key, stackbuf[:]); err != nil {
//					return -1
//				} else {
//					keyUnesc = ku
//				}
//
//				if level <= len(keys) {
//					if equalStr(&keyUnesc, keys[level-1]) {
//						lastMatched = true
//
//						// if key level match
//						if keyLevel == level-1 {
//							keyLevel++
//							// If we found all keys in path
//							if keyLevel == lk {
//								return i + 1
//							}
//						}
//					} else {
//						lastMatched = false
//					}
//				} else {
//					return -1
//				}
//			} else {
//				i--
//			}
//		case '{':
//
//			// in case parent key is matched then only we will increase the level otherwise can directly
//			// can move to the end of this block
//			if !lastMatched {
//				end := blockEnd(data[i:], '{', '}')
//				if end == -1 {
//					return -1
//				}
//				i += end - 1
//			} else {
//				level++
//			}
//		case '}':
//			level--
//			if level == keyLevel {
//				keyLevel--
//			}
//		case '[':
//			// If we want to get array element by index
//			if keyLevel == level && keys[level][0] == '[' {
//				var keyLen = len(keys[level])
//				if keyLen < 3 || keys[level][0] != '[' || keys[level][keyLen-1] != ']' {
//					return -1
//				}
//				aIdx, err := strconv.Atoi(keys[level][1 : keyLen-1])
//				if err != nil {
//					return -1
//				}
//				var curIdx int
//				var valueFound []byte
//				var valueOffset int
//				var curI = i
//				ArrayEach(data[i:], func(value []byte, dataType ValueType, offset int, err error) {
//					if curIdx == aIdx {
//						valueFound = value
//						valueOffset = offset
//						if dataType == RString {
//							valueOffset = valueOffset - 2
//							valueFound = data[curI+valueOffset : curI+valueOffset+len(value)+2]
//						}
//					}
//					curIdx += 1
//				})
//
//				if valueFound == nil {
//					return -1
//				} else {
//					subIndex := searchKeys(valueFound, keys[level+1:]...)
//					if subIndex < 0 {
//						return -1
//					}
//					return i + valueOffset + subIndex
//				}
//			} else {
//				// Do not search for keys inside arrays
//				if arraySkip := blockEnd(data[i:], '[', ']'); arraySkip == -1 {
//					return -1
//				} else {
//					i += arraySkip - 1
//				}
//			}
//		case ':': // If encountered, JSON data is malformed
//			return -1
//		}
//
//		i++
//	}
//
//	return -1
//}
//
//func sameTree(p1, p2 []string) bool {
//	minLen := len(p1)
//	if len(p2) < minLen {
//		minLen = len(p2)
//	}
//
//	for pi_1, p_1 := range p1[:minLen] {
//		if p2[pi_1] != p_1 {
//			return false
//		}
//	}
//
//	return true
//}

// Data types available in valid JSON data.
type ValueType int

const (
	NotExist = ValueType(iota)
	RString
	Number
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
	case RString:
		return "string"
	case Number:
		return "number"
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

func calcAllocateSpace(keys []string, setValue []byte, comma, object bool) int {
	isIndex := string(keys[0][0]) == "["
	lk := 0
	if comma {
		// ,
		lk += 1
	}
	if isIndex && !comma {
		// []
		lk += 2
	} else {
		if object {
			// {
			lk += 1
		}
		if !isIndex {
			// "keys[0]"
			lk += len(keys[0]) + 3
		}
	}

	lk += len(setValue)
	for i := 1; i < len(keys); i++ {
		if string(keys[i][0]) == "[" {
			// []
			lk += 2
		} else {
			// {"keys[i]":setValue}
			lk += len(keys[i]) + 5
		}
	}

	if object && !isIndex {
		// }
		lk += 1
	}

	return lk
}

func WriteToBuffer(buffer []byte, str string) int {
	copy(buffer, str)
	return len(str)
}

func getType(data []byte, offset int) ([]byte, ValueType, int, int, error) {
	var dataType ValueType
	endOffset := offset
	//fmt.Println("Get type", string(data))
	// if string value
	if data[offset] == '"' {
		dataType = RString
		if idx, _ := stringEnd(data[offset+1:]); idx != -1 {
			endOffset += idx + 1
		} else {
			return nil, dataType, offset, 0, MalformedStringError
		}
		//endOffset := int(binary.LittleEndian.Uint16(data[offset+1 : offset+3]))
		////fmt.Println(string(data))
		////fmt.Println(data[offset+1 : offset+3])
		////fmt.Println("byte length", endOffset)
		//offset += 2
		//endOffset += offset
		//return data[offset:endOffset], dataType, endOffset, 0, nil
	} else if data[offset] == '(' {
		dataType = Array
		//fmt.Println("Detected Array")
		arrLength := binary.LittleEndian.Uint16(data[offset+1 : offset+3])
		endOffset := int(binary.LittleEndian.Uint16(data[offset+3 : offset+5]))
		//fmt.Println(data[offset+1 : offset+5])
		//fmt.Println("byte length", endOffset)
		offset += 4
		endOffset += offset
		data[offset] = '['
		//fmt.Println("Array ", string(data[offset:endOffset]))
		return data[offset:endOffset], dataType, endOffset, int(arrLength), nil
	} else if data[offset] == '[' { // if array value
		dataType = Array
		//fmt.Println("Detected Array")

		// break label, for stopping nested loops
		endOffset = blockEnd(data[offset:], '[', ']')
		if endOffset == -1 {
			return nil, dataType, offset, 0, MalformedArrayError
		}

		endOffset += offset
	} else if data[offset] == '{' { // if object value
		dataType = Object
		// break label, for stopping nested loops
		endOffset = blockEnd(data[offset:], '{', '}')

		if endOffset == -1 {
			return nil, dataType, offset, 0, MalformedObjectError
		}

		endOffset += offset
	} else if data[offset] == '<' {
		dataType = Object
		arrLength := binary.LittleEndian.Uint16(data[offset+1 : offset+3])
		endOffset := int(binary.LittleEndian.Uint16(data[offset+3 : offset+5]))
		//fmt.Println("endoffset ", endOffset)
		offset += 4
		endOffset += offset
		data[offset] = '{'
		//fmt.Println("Object", string(data[offset:endOffset]))
		return data[offset:endOffset], dataType, endOffset, int(arrLength), nil
	} else {
		// Number, Boolean or None
		end := tokenEnd(data[endOffset:])

		if end == -1 {
			return nil, dataType, offset, 0, MalformedValueError
		}

		value := data[offset : endOffset+end]

		switch data[offset] {
		case 't', 'f': // true or false
			if bytes.Equal(value, trueLiteral) || bytes.Equal(value, falseLiteral) {
				dataType = Boolean
			} else {
				return nil, Unknown, offset, 0, UnknownValueTypeError
			}
		case 'u', 'n': // undefined or null
			if bytes.Equal(value, nullLiteral) {
				dataType = Null
			} else {
				return nil, Unknown, offset, 0, UnknownValueTypeError
			}
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-':
			dataType = Number
		default:
			return nil, Unknown, offset, 0, UnknownValueTypeError
		}

		endOffset += end
	}
	return data[offset:endOffset], dataType, endOffset, 0, nil
}

/*
Get - Receives data structure, and key path to extract value from.

Returns:
`value` - Pointer to original data structure containing key value, or just empty slice if nothing found or error
`dataType` -    Can be: `NotExist`, `String`, `Number`, `Object`, `Array`, `Boolean` or `Null`
`offset` - Offset from provided data structure where key value ends. Used mostly internally, for example for `ArrayEach` helper.
`err` - If key not found or any other parsing issue it should return error. If key not found it also sets `dataType` to `NotExist`

Accept multiple keys to specify path to JSON value (in case of quering nested structures).
If no keys provided it will try to extract closest JSON value (simple ones or object/array), useful for reading streams or arrays, see `ArrayEach` implementation.
*/
func Get(data []byte, keys ...string) (value []byte, dataType ValueType, offset, sz int, err error) {
	a, b, _, d, sz, e := internalGet(data, keys...)
	return a, b, d, sz, e
}

func internalGet(data []byte, keys ...string) (value []byte, dataType ValueType, offset, endOffset, sz int, err error) {

	// Go to closest value
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
	if dataType == RString {
		value = value[1 : len(value)-1]
	}

	return value[:len(value):len(value)], dataType, offset, endOffset, sz, nil
}

// ArrayEach is used when iterating arrays, accepts a callback function with the same return arguments as `Get`.
func ArrayEach(data []byte, cb func(value []byte, dataType ValueType, offset, sz int, err error), keys ...string) (offset int, err error) {
	if len(data) == 0 {
		return -1, MalformedObjectError
	}

	//fmt.Println("Array each", string(data))
	nT := nextToken(data)
	if nT == -1 {
		return -1, MalformedJsonError
	}

	offset = nT + 1
	//if len(keys) > 0 {
	//	if offset = searchKeys(data, keys...); offset == -1 {
	//		return offset, KeyPathNotFoundError
	//	}
	//
	//	// Go to closest value
	//	nO := nextToken(data[offset:])
	//	if nO == -1 {
	//		return offset, MalformedJsonError
	//	}
	//
	//	offset += nO
	//
	//	if data[offset] != '[' {
	//		return offset, MalformedArrayError
	//	}
	//
	//	offset++
	//}

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
		//fmt.Println(v, t, o, e)
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

	// Descend to the desired key, if requested
	//if len(keys) > 0 {
	//	if off := searchKeys(data, keys...); off == -1 {
	//		return KeyPathNotFoundError
	//	} else {
	//		offset = off
	//	}
	//}

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

// ParseBoolean parses a Boolean ValueType into a Go bool (not particularly useful, but here for completeness)
func ParseBoolean(b []byte) (bool, error) {
	switch {
	case bytes.Equal(b, trueLiteral):
		return true, nil
	case bytes.Equal(b, falseLiteral):
		return false, nil
	default:
		return false, MalformedValueError
	}
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

// ParseNumber parses a Number ValueType into a Go float64
func ParseFloat(b []byte) (float64, error) {
	if v, err := parseFloat(&b); err != nil {
		return 0, MalformedValueError
	} else {
		return v, nil
	}
}

// ParseInt parses a Number ValueType into a Go int64
func ParseInt(b []byte) (int64, error) {
	if v, ok, overflow := parseInt(b); !ok {
		if overflow {
			return 0, OverflowIntegerError
		}
		return 0, MalformedValueError
	} else {
		return v, nil
	}
}
