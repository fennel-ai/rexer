package value

import (
	"fmt"
	"reflect"
	"strings"
)

type Value interface {
	isValue()
	Equal(v Value) bool
	Op(opt string, other Value) (Value, error)
	String() string
	Clone() Value
}

var _ Value = Int(0)
var _ Value = Double(0)
var _ Value = Bool(true)
var _ Value = String("")
var _ Value = List([]Value{Int(0), Bool(true)})
var _ Value = Dict(map[string]Value{"hi": Int(0), "bye": Bool(true)})
var _ Value = nil_{}
var _ Value = Table{}

type Int int64

func (I Int) isValue() {}
func (I Int) Equal(v Value) bool {
	switch v.(type) {
	case Int:
		return v.(Int) == I
	default:
		return false
	}
}
func (I Int) String() string {
	return fmt.Sprintf("Int(%v)", int32(I))
}
func (I Int) Clone() Value {
	return Int(I)
}
func (I Int) Op(opt string, other Value) (Value, error) {
	return route(I, opt, other)
}

type Double float64

func (d Double) isValue() {}
func (d Double) Equal(v Value) bool {
	switch v.(type) {
	case Double:
		return v.(Double) == d
	default:
		return false
	}
}
func (d Double) String() string {
	return fmt.Sprintf("Double(%v)", float64(d))
}
func (d Double) Clone() Value {
	return Double(d)
}
func (d Double) Op(opt string, other Value) (Value, error) {
	return route(d, opt, other)
}

type Bool bool

func (b Bool) isValue() {}
func (b Bool) Equal(v Value) bool {
	switch v.(type) {
	case Bool:
		return v.(Bool) == b
	default:
		return false
	}
}
func (b Bool) String() string {
	return fmt.Sprintf("Bool(%v)", bool(b))
}
func (b Bool) Clone() Value {
	return Bool(b)
}

func (b Bool) Op(opt string, other Value) (Value, error) {
	return route(b, opt, other)
}

type String string

func (s String) isValue() {}
func (s String) Equal(v Value) bool {
	switch v.(type) {
	case String:
		return v.(String) == s
	default:
		return false
	}
}
func (s String) String() string {
	return fmt.Sprintf("String(%s)", string(s))
}
func (s String) Clone() Value {
	return String(s)
}
func (s String) Op(opt string, other Value) (Value, error) {
	return route(s, opt, other)
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
	return fmt.Sprintf("Nil")
}
func (n nil_) Clone() Value {
	return Nil
}
func (n nil_) Op(opt string, other Value) (Value, error) {
	return route(n, opt, other)
}

type List []Value

func (l List) Op(opt string, other Value) (Value, error) {
	return route(l, opt, other)
}

func NewList(values []Value) (Value, error) {
	ret := make([]Value, 0, len(values))
	for _, v := range values {
		ret = append(ret, v)
	}
	return List(ret), nil
}
func (l List) isValue() {}
func (l List) Equal(right Value) bool {
	switch right.(type) {
	case List:
		r := right.(List)
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
	for _, v := range l {
		s := fmt.Sprintf("%v, ", v.String())
		sb.WriteString(s)
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

type Dict map[string]Value

func (d Dict) Op(opt string, other Value) (Value, error) {
	return route(d, opt, other)
}

func NewDict(values map[string]Value) (Dict, error) {
	ret := make(map[string]Value, len(values))
	for k, v := range values {
		ret[k] = v
	}
	return Dict(ret), nil
}

func (d Dict) isValue() {}
func (d Dict) Equal(v Value) bool {
	switch v.(type) {
	case Dict:
		right := v.(Dict)
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
	sb := strings.Builder{}
	sb.WriteString("{")
	for k, v := range d {
		sb.WriteString(fmt.Sprintf("%s: %v, ", k, v.String()))

	}
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

func (d Dict) flatten() Dict {
	ret := make(map[string]Value)
	for k, v := range d {
		switch v.(type) {
		case Dict:
			for k2, v2 := range v.(Dict).flatten() {
				knew := fmt.Sprintf("%s.%s", k, k2)
				ret[knew] = v2
			}
		default:
			ret[k] = v
		}
	}
	return ret
}

func (d Dict) schema() map[string]reflect.Type {
	fd := d.flatten()
	ret := make(map[string]reflect.Type, len(fd))
	for k, v := range d {
		ret[k] = reflect.TypeOf(v)
	}
	return ret
}

type Table struct {
	// TODO: don't store each row as Dict but rather as Value array
	schema map[string]reflect.Type
	rows   []Dict
}

func NewTable() Table {
	return Table{nil, make([]Dict, 0)}
}

func (t *Table) Append(row Dict) error {
	row = row.flatten()
	if len(t.rows) == 0 {
		t.schema = row.schema()
	} else if !t.schemaMatches(row.schema()) {
		return fmt.Errorf("can not append row to table: scheams don't match")
	}
	t.rows = append(t.rows, row)
	return nil
}

func (t *Table) Pull() []Dict {
	return t.rows
}

func (t Table) schemaMatches(schema map[string]reflect.Type) bool {
	if len(t.schema) != len(schema) {
		return false
	}
	for k, v := range t.schema {
		if v != schema[k] {
			return false
		}
	}
	return true
}

func (t Table) isValue() {}

func (t Table) Equal(v Value) bool {
	switch v.(type) {
	case Table:
		other := v.(Table)
		if len(t.rows) != len(other.rows) {
			return false
		}
		for i, row := range t.rows {
			if !row.Equal(other.rows[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (t Table) Op(opt string, other Value) (Value, error) {
	return route(t, opt, other)
}

func (t Table) String() string {
	var sb strings.Builder
	sb.WriteString("table")
	sb.WriteRune('(')
	for _, row := range t.rows {
		sb.WriteString(row.String())
		sb.WriteRune(',')
	}
	sb.WriteRune(')')
	return sb.String()
}

func (t Table) Clone() Value {
	ret := NewTable()
	for _, row := range t.rows {
		cloned := row.Clone().(Dict)
		ret.Append(cloned)
	}
	return ret
}
