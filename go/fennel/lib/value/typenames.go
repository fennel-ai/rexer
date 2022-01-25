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
