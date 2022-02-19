package value

import "reflect"

type _types struct {
	Int    reflect.Type
	Double reflect.Type
	String reflect.Type
	Bool   reflect.Type
	List   reflect.Type
	Dict   reflect.Type
	Table  reflect.Type
	Any    reflect.Type
}

var Types _types

func init() {
	Types = _types{
		Int:    reflect.TypeOf(Int(1)),
		String: reflect.TypeOf(String("hi")),
		Bool:   reflect.TypeOf(Bool(true)),
		Double: reflect.TypeOf(Double(1.0)),
		List:   reflect.TypeOf(List{Int(1), Double(3.4)}),
		Dict:   reflect.TypeOf(Dict{}),
		Table:  reflect.TypeOf(Table{}),
		Any:    reflect.TypeOf((*Value)(nil)).Elem(),
	}
}

func (ts _types) ToString(t reflect.Type) string {
	switch t {
	case Types.Bool:
		return "Bool"
	case Types.Int:
		return "Int"
	case Types.Double:
		return "Double"
	case Types.String:
		return "String"
	case Types.List:
		return "List"
	case Types.Dict:
		return "Dict"
	case Types.Table:
		return "Table"
	case Types.Any:
		return "Any"
	default:
		return "Unknown"
	}
}
