package value

import (
	"fmt"
	"reflect"
)

type Type interface {
	Validate(Value) error
	String() string
}

var _ Type = anyType{}
var _ Type = baseType{}
var _ Type = compoundType{}
var _ Type = listType{}
var _ Type = dictType{}

type anyType struct{}

func (at anyType) Validate(Value) error {
	return nil
}

func (at anyType) String() string {
	return "Any"
}

type baseType struct {
	name  string
	_type reflect.Type
}

func (bt baseType) Validate(v Value) error {
	if bt._type != reflect.TypeOf(v) {
		return fmt.Errorf("value '%s' does not satisfy expected type '%s'", v, bt)
	}
	return nil
}

func (bt baseType) String() string {
	return bt.name
}

type compoundType struct {
	name   string
	_types []Type
}

func (ct compoundType) Validate(v Value) error {
	for _, t := range ct._types {
		if err := t.Validate(v); err == nil {
			return nil
		}
	}
	return fmt.Errorf("value '%s' does not satisfy expected type '%s'", v, ct)
}

func (ct compoundType) String() string {
	return ct.name
}

type listType struct {
	name     string
	elemType Type
}

func (lt listType) Validate(v Value) error {
	l, ok := v.(List)
	if !ok {
		return fmt.Errorf("expected a list but found '%s'", v)
	}
	for i := 0; i < l.Len(); i++ {
		v, _ := l.At(i)
		if err := lt.elemType.Validate(v); err != nil {
			return fmt.Errorf("value '%s' of element '%d' of list does not satisfy expected type '%s'",
				v, i, lt.elemType)
		}
	}
	return nil
}

func (lt listType) String() string {
	return lt.name
}

type dictType struct {
	name     string
	elemType Type
}

func (dt dictType) Validate(v Value) error {
	d, ok := v.(Dict)
	if !ok {
		return fmt.Errorf("expected a dict but found '%s'", v)
	}
	for k := range d.Iter() {
		v, _ := d.Get(k)
		if err := dt.elemType.Validate(v); err != nil {
			return fmt.Errorf("value '%s' of key '%s' of dict does not satisfy expected type '%s'",
				v, k, dt.elemType)
		}
	}
	return nil
}

func (dt dictType) String() string {
	return dt.name
}

type typeIndex struct {
	// Base types
	Int    Type
	Double Type
	String Type
	Bool   Type
	List   Type
	Dict   Type
	Any    Type
	// Other types
	Number        Type
	ListOfBools   Type
	ListOfNumbers Type
}

var Types typeIndex

func init() {
	Types = typeIndex{}
	Types.Any = anyType{}
	// Set base types
	Types.Bool = baseType{"Bool", reflect.TypeOf(Bool(true))}
	Types.Int = baseType{"Int", reflect.TypeOf(Int(1))}
	Types.Double = baseType{"Double", reflect.TypeOf(Double(1.0))}
	Types.String = baseType{"String", reflect.TypeOf(String("hi"))}
	Types.List = baseType{"List", reflect.TypeOf(NewList(Int(1), Double(3.4)))}
	Types.Dict = baseType{"Dict", reflect.TypeOf(Dict{})}
	// Set other types (ensure subtypes are set before using them)
	Types.Number = compoundType{"Number", []Type{Types.Int, Types.Double}}
	Types.ListOfBools = listType{name: "List of Bools", elemType: Types.Bool}
	Types.ListOfNumbers = listType{name: "List of Numbers", elemType: Types.Number}
}
