package value

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type Value interface {
	isValue()
	Equal(v Value) bool
	Op(opt string, other Value) (Value, error)
	// String is used to construct aggregation keys so don't ever change
	// this presentation logic because that could invalidate existing keys
	// also, each value should return a unique & non-ambiguous string
	String() string
	Clone() Value
	MarshalJSON() ([]byte, error)
}

var _ Value = Int(0)
var _ Value = Double(0)
var _ Value = Bool(true)
var _ Value = String("")
var _ Value = List([]Value{Int(0), Bool(true)})
var _ Value = Dict(map[string]Value{"hi": Int(0), "bye": Bool(true)})
var _ Value = nil_{}

type Int int64

func (I Int) isValue() {}
func (I Int) Equal(v Value) bool {
	switch v := v.(type) {
	case Int:
		return v == I
	case Double:
		return float64(v) == float64(I)
	default:
		return false
	}
}
func (I Int) String() string {
	return strconv.FormatInt(int64(I), 10)
}
func (I Int) Clone() Value {
	return I
}
func (I Int) Op(opt string, other Value) (Value, error) {
	return route(I, opt, other)
}
func (I Int) MarshalJSON() ([]byte, error) {
	return []byte(I.String()), nil
}

type Double float64

func (d Double) isValue() {}
func (d Double) Equal(v Value) bool {
	switch v := v.(type) {
	case Int:
		return float64(v) == float64(d)
	case Double:
		return v == d
	default:
		return false
	}
}
func (d Double) String() string {
	// here we take the minimum number of decimal places needed to represent the float
	// so 3.4 is represented as just that, not 3.400000
	// this helps keep the representation unique
	// Integral floats are to be differentiated from integers
	// so 2 is represented as 2.0
	str := strconv.FormatFloat(float64(d), 'f', -1, 64)
	for i := range str {
		if str[i] == '.' {
			return str
		}
	}
	sb := strings.Builder{}
	sb.WriteString(str)
	sb.WriteString(".0")
	return sb.String()
}
func (d Double) Clone() Value {
	return d
}
func (d Double) Op(opt string, other Value) (Value, error) {
	return route(d, opt, other)
}
func (d Double) MarshalJSON() ([]byte, error) {
	return []byte(d.String()), nil
}

type Bool bool

func (b Bool) isValue() {}
func (b Bool) Equal(v Value) bool {
	switch v := v.(type) {
	case Bool:
		return v == b
	default:
		return false
	}
}
func (b Bool) String() string {
	return strconv.FormatBool(bool(b))
}
func (b Bool) Clone() Value {
	return b
}
func (b Bool) Op(opt string, other Value) (Value, error) {
	return route(b, opt, other)
}
func (b Bool) MarshalJSON() ([]byte, error) {
	return []byte(b.String()), nil
}

type String string

func (s String) isValue() {}
func (s String) Equal(v Value) bool {
	switch v := v.(type) {
	case String:
		return v == s
	default:
		return false
	}
}
func (s String) String() string {
	sb := strings.Builder{}
	sb.WriteString(`"`)
	sb.WriteString(string(s))
	sb.WriteString(`"`)
	return sb.String()
}
func (s String) Clone() Value {
	return s
}
func (s String) Op(opt string, other Value) (Value, error) {
	return route(s, opt, other)
}
func (s String) MarshalJSON() ([]byte, error) {
	return []byte(s.String()), nil
}

type nil_ struct{}

var Nil = nil_{}

func (n nil_) isValue() {}
func (n nil_) Equal(v Value) bool {
	switch v.(type) {
	case nil_:
		return true
	default:
		return false
	}
}
func (n nil_) String() string {
	return "null"
}
func (n nil_) Clone() Value {
	return Nil
}
func (n nil_) Op(opt string, other Value) (Value, error) {
	return route(n, opt, other)
}
func (n nil_) MarshalJSON() ([]byte, error) {
	return []byte(n.String()), nil
}

type List []Value

func (l List) Op(opt string, other Value) (Value, error) {
	return route(l, opt, other)
}

func NewList(values []Value) List {
	ret := make([]Value, 0, len(values))
	for _, v := range values {
		ret = append(ret, v)
	}
	return ret
}

func (l List) isValue() {}
func (l List) Equal(right Value) bool {
	switch r := right.(type) {
	case List:
		if len(r) != len(l) {
			return false
		}
		for i, lv := range l {
			if !lv.Equal(r[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
func (l List) String() string {
	sb := strings.Builder{}
	sb.WriteString("[")
	for i, v := range l {
		if v == nil {
			sb.WriteString("null")
		} else {
			sb.WriteString(v.String())
		}
		if i != len(l)-1 {
			sb.WriteString(",")
		}
	}
	sb.WriteString("]")
	return sb.String()
}
func (l List) Clone() Value {
	clone := make([]Value, 0, len(l))

	for _, v := range l {
		clone = append(clone, v.Clone())
	}
	return List(clone)
}
func (l List) MarshalJSON() ([]byte, error) {
	return []byte(l.String()), nil
}

func (l *List) Append(v Value) error {
	*l = append(*l, v)
	return nil
}
func (l *List) Iter() Iter {
	return Iter{0, *l}
}

type Dict map[string]Value

func (d Dict) Op(opt string, other Value) (Value, error) {
	return route(d, opt, other)
}

func NewDict(values map[string]Value) (Dict, error) {
	ret := make(map[string]Value, len(values))
	for k, v := range values {
		ret[k] = v
	}
	return ret, nil
}

func (d Dict) isValue() {}
func (d Dict) Equal(v Value) bool {
	switch right := v.(type) {
	case Dict:
		if len(right) != len(d) {
			return false
		}
		for k, lv := range d {
			if rv, ok := right[k]; !(ok && lv.Equal(rv)) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
func (d Dict) String() string {
	s := make([]string, 0, len(d))
	for k, v := range d {
		sb := strings.Builder{}
		sb.WriteString(`"`)
		sb.WriteString(k)
		sb.WriteString(`"`)
		sb.WriteString(":")
		if v == nil {
			sb.WriteString("null")
		} else {
			sb.WriteString(v.String())
		}
		s = append(s, sb.String())
	}
	// we sort these strings so that each dictionary gets a unique representation
	sort.Strings(s)
	sb := strings.Builder{}
	sb.WriteString("{")
	sb.WriteString(strings.Join(s, ","))
	sb.WriteString("}")
	return sb.String()
}
func (d Dict) Clone() Value {
	clone := make(map[string]Value, len(d))
	for k, v := range d {
		clone[k] = v.Clone()
	}
	return Dict(clone)
}
func (d Dict) MarshalJSON() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d Dict) Schema() map[string]reflect.Type {
	ret := make(map[string]reflect.Type, len(d))
	for k, v := range d {
		ret[k] = reflect.TypeOf(v)
	}
	return ret
}

type Iter struct {
	next int
	rows List
}

func (iter *Iter) HasMore() bool {
	return iter.next < len(iter.rows)
}

func (iter *Iter) Next() (Value, error) {
	curr := iter.next
	if curr >= len(iter.rows) {
		return nil, fmt.Errorf("exhaused iter - no more items to iterate upon")
	}
	iter.next += 1
	return iter.rows[curr], nil
}
