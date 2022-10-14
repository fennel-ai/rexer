package schema

type Schema struct {
	Name   string
	Fields []Field
}

type Field struct {
	Name         string
	DataType     DataType
	Nullable     bool
	DefaultValue Value
}

type DataType interface {
	IsDataType()
}

var _ DataType = ScalarType(0)
var _ DataType = ArrayType{}
var _ DataType = MapType{}

type ScalarType int

func (s ScalarType) IsDataType() {}

type ArrayType struct {
	ValueType DataType
}

func (a ArrayType) IsDataType() {}

type MapType struct {
	KeyType   DataType
	ValueType DataType
}

func (m MapType) IsDataType() {}

type Value interface {
	IsValue()
}

type Int int64

func (i Int) IsValue() {}

type Double float64

func (d Double) IsValue() {}

type String string

func (s String) IsValue() {}

type Bool bool

func (b Bool) IsValue() {}

type Timestamp struct {
	Seconds int64
	Nanos   int32
	Now     bool
}

func (t Timestamp) IsValue() {}

type Array struct {
	Elements []Value
}

func (a Array) IsValue() {}

type Map struct {
	Keys   []Value
	Values []Value
}

func (m Map) IsValue() {}
