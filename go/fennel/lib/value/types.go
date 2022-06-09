package value

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"fennel/lib/utils/slice"
)

type Value interface {
	isValue()
	Equal(v Value) bool
	Op(opt string, other Value) (Value, error)
	OpUnary(opt string) (Value, error)
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
var _ Value = List{}
var _ Value = Dict{}
var _ Value = nil_{}
var _ Value = &Future{}

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
func (I Int) OpUnary(opt string) (Value, error) {
	return routeUnary(opt, I)
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
func (d Double) OpUnary(opt string) (Value, error) {
	return routeUnary(opt, d)
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
func (b Bool) OpUnary(opt string) (Value, error) {
	return routeUnary(opt, b)
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
func (s String) OpUnary(opt string) (Value, error) {
	return routeUnary(opt, s)
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
func (n nil_) OpUnary(opt string) (Value, error) {
	return routeUnary(opt, n)
}
func (n nil_) MarshalJSON() ([]byte, error) {
	return []byte(n.String()), nil
}

// TODO: hide internal details of List struct so people can not create lists without using
// NewList. That way, we can ensure that no one creates nested lists
type List struct {
	values []Value
}

func (l List) Op(opt string, other Value) (Value, error) {
	return route(l, opt, other)
}
func (l List) OpUnary(opt string) (Value, error) {
	return routeUnary(opt, l)
}

func NewList(values ...Value) List {
	if len(values) == 0 {
		return List{values: []Value{}}
	} else {
		return List{values: values}
	}
}

func (l List) isValue() {}
func (l List) Equal(right Value) bool {
	switch r := right.(type) {
	case List:
		if len(r.values) != len(l.values) {
			return false
		}
		for i, lv := range l.values {
			if !lv.Equal(r.values[i]) {
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
	for i, v := range l.values {
		if v == nil {
			sb.WriteString("null")
		} else {
			sb.WriteString(v.String())
		}
		if i != len(l.values)-1 {
			sb.WriteString(",")
		}
	}
	sb.WriteString("]")
	return sb.String()
}
func (l List) Clone() Value {
	clone := make([]Value, 0, len(l.values))

	for _, v := range l.values {
		clone = append(clone, v.Clone())
	}
	return NewList(clone...)
}
func (l List) MarshalJSON() ([]byte, error) {
	return []byte(l.String()), nil
}

func (l *List) Append(vals ...Value) {
	l.values = append(l.values, vals...)
}

func (l *List) Grow(n int) {
	l.values = slice.Grow(l.values, n)
}

func (l *List) Iter() Iter {
	return Iter{0, *l}
}

func (l List) Len() int {
	return len(l.values)
}

func (l List) At(idx int) (Value, error) {
	if idx < 0 || idx >= l.Len() {
		return nil, fmt.Errorf("index '%d' out of bounds for list of length: '%d'", idx, l.Len())
	}
	return l.values[idx], nil
}

func (l List) Values() []Value {
	return l.values
}

type Dict struct {
	values map[string]Value
}

func (d Dict) Op(opt string, other Value) (Value, error) {
	return route(d, opt, other)
}
func (d Dict) OpUnary(opt string) (Value, error) {
	return routeUnary(opt, d)
}

func NewDict(values map[string]Value) Dict {
	if len(values) == 0 {
		return Dict{values: map[string]Value{}}
	} else {
		return Dict{values: values}
	}
}

func (d Dict) Len() int {
	return len(d.values)
}

func (d Dict) Get(k string) (Value, bool) {
	v, ok := d.values[k]
	return v, ok
}

func (d Dict) GetUnsafe(k string) Value {
	if v, ok := d.values[k]; ok {
		return v
	}
	return nil
}

func (d *Dict) Set(k string, value Value) {
	d.values[k] = value
}

func (d Dict) Iter() map[string]Value {
	return d.values
}
func (d *Dict) Del(k string) {
	delete(d.values, k)
}

func (d Dict) isValue() {}
func (d Dict) Equal(v Value) bool {
	switch right := v.(type) {
	case Dict:
		if right.Len() != d.Len() {
			return false
		}
		for k, lv := range d.Iter() {
			if rv, ok := right.Get(k); !(ok && lv.Equal(rv)) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
func (d Dict) String() string {
	s := make([]string, 0, d.Len())
	for k, v := range d.Iter() {
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
	clone := make(map[string]Value, d.Len())
	for k, v := range d.Iter() {
		clone[k] = v.Clone()
	}
	return NewDict(clone)
}
func (d Dict) MarshalJSON() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d Dict) Schema() map[string]reflect.Type {
	ret := make(map[string]reflect.Type, d.Len())
	for k, v := range d.Iter() {
		ret[k] = reflect.TypeOf(v)
	}
	return ret
}

type Iter struct {
	next int
	rows List
}

func (iter *Iter) HasMore() bool {
	return iter.next < len(iter.rows.values)
}

func (iter *Iter) Next() (Value, error) {
	curr := iter.next
	if curr >= len(iter.rows.values) {
		return nil, fmt.Errorf("exhaused iter - no more items to iterate upon")
	}
	iter.next += 1
	return iter.rows.values[curr], nil
}

type Future struct {
	lock   sync.Mutex
	ch     <-chan Value
	cached Value
}

func NewFuture(ch <-chan Value) Future {
	return Future{
		lock:   sync.Mutex{},
		ch:     ch,
		cached: nil,
	}
}

func (f *Future) await() Value {
	if f.cached != nil {
		return f.cached
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.cached = <-f.ch
	return f.cached
}

func (f *Future) isValue() {}

func (f *Future) Equal(v Value) bool {
	return f.await().Equal(v)
}

func (f *Future) Op(opt string, other Value) (Value, error) {
	return f.await().Op(opt, other)
}
func (f *Future) OpUnary(opt string) (Value, error) {
	return f.await().OpUnary(opt)
}

func (f *Future) String() string {
	return f.await().String()
}

func (f *Future) Clone() Value {
	return f.await().Clone()
}

func (f *Future) MarshalJSON() ([]byte, error) {
	return f.await().MarshalJSON()
}
