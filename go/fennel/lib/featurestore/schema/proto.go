package schema

import (
	"log"

	"fennel/lib/featurestore/schema/proto"
	gproto "google.golang.org/protobuf/proto"
)

func Serialize(schema Schema) ([]byte, error) {
	return gproto.Marshal(ToProto(schema))
}

func Deserialize(data []byte) (Schema, error) {
	var pschema proto.Schema
	err := gproto.Unmarshal(data, &pschema)
	if err != nil {
		return Schema{}, err
	}
	return FromProto(&pschema), nil
}

func FromProto(pschema *proto.Schema) Schema {
	return Schema{
		Name:   pschema.Name,
		Fields: fromProtoFields(pschema.Fields),
	}
}

func ToProto(schema Schema) *proto.Schema {
	return &proto.Schema{
		Name:   schema.Name,
		Fields: toProtoFields(schema.Fields),
	}
}

func fromProtoFields(pfields []*proto.Field) []Field {
	fields := make([]Field, len(pfields))
	for i := range fields {
		fields[i] = fromProtoField(pfields[i])
	}
	return fields
}

func toProtoFields(fields []Field) []*proto.Field {
	pfields := make([]*proto.Field, len(fields))
	for i := range fields {
		pfields[i] = toProtoField(fields[i])
	}
	return pfields
}

func fromProtoField(pfield *proto.Field) Field {
	return Field{
		Name:         pfield.Name,
		DataType:     fromProtoDataType(pfield.Dtype),
		Nullable:     pfield.Nullable,
		DefaultValue: fromProtoValue(pfield.DefaultValue),
	}
}

func toProtoField(field Field) *proto.Field {
	return &proto.Field{
		Name:         field.Name,
		Dtype:        toProtoDataType(field.DataType),
		Nullable:     field.Nullable,
		DefaultValue: toProtoValue(field.DefaultValue),
		Expectation:  nil, // add code when expectation is added properly
	}
}

func fromProtoDataType(pdt *proto.DataType) DataType {
	switch pdt.Type.(type) {
	case *proto.DataType_ScalarType:
		return ScalarType(pdt.GetScalarType())
	case *proto.DataType_ArrayType:
		return fromProtoArrayType(pdt.GetArrayType())
	case *proto.DataType_MapType:
		return fromProtoMapType(pdt.GetMapType())
	default:
		// this should never happen as all types are covered
		log.Println("schema.fromProtoDataType found unknown type which should never happen")
		return ScalarType(0)
	}
}

func toProtoDataType(dt DataType) *proto.DataType {
	switch dt := dt.(type) {
	case ScalarType:
		return &proto.DataType{Type: &proto.DataType_ScalarType{ScalarType: proto.ScalarType(dt)}}
	case ArrayType:
		return &proto.DataType{Type: &proto.DataType_ArrayType{ArrayType: toProtoArrayType(dt)}}
	case MapType:
		return &proto.DataType{Type: &proto.DataType_MapType{MapType: toProtoMapType(dt)}}
	default:
		// this should never happen as all types are covered
		log.Print("schema.toProtoDataType found unknown type which should never happen")
		return &proto.DataType{Type: &proto.DataType_ScalarType{ScalarType: proto.ScalarType_INT}}
	}
}

func fromProtoArrayType(pat *proto.ArrayType) ArrayType {
	return ArrayType{ValueType: fromProtoDataType(pat.Of)}
}

func toProtoArrayType(at ArrayType) *proto.ArrayType {
	return &proto.ArrayType{Of: toProtoDataType(at.ValueType)}
}

func fromProtoMapType(pmt *proto.MapType) MapType {
	return MapType{
		KeyType:   fromProtoDataType(pmt.Key),
		ValueType: fromProtoDataType(pmt.Value),
	}
}

func toProtoMapType(mt MapType) *proto.MapType {
	return &proto.MapType{
		Key:   toProtoDataType(mt.KeyType),
		Value: toProtoDataType(mt.ValueType),
	}
}

func fromProtoValue(pv *proto.Value) Value {
	switch pv.Value.(type) {
	case *proto.Value_IntValue:
		return Int(pv.GetIntValue())
	case *proto.Value_DoubleValue:
		return Double(pv.GetDoubleValue())
	case *proto.Value_StringValue:
		return String(pv.GetStringValue())
	case *proto.Value_BoolValue:
		return Bool(pv.GetBoolValue())
	case *proto.Value_TimestampValue:
		return fromProtoTimestamp(pv.GetTimestampValue())
	case *proto.Value_ArrayValue:
		return fromProtoArray(pv.GetArrayValue())
	case *proto.Value_MapValue:
		return fromProtoMap(pv.GetMapValue())
	default:
		// this should never happen as all types are covered
		log.Println("schema.fromProtoValue found unknown type which should never happen")
		return Int(0)
	}
}

func toProtoValue(v Value) *proto.Value {
	switch v := v.(type) {
	case Int:
		return &proto.Value{Value: &proto.Value_IntValue{IntValue: int64(v)}}
	case Double:
		return &proto.Value{Value: &proto.Value_DoubleValue{DoubleValue: float64(v)}}
	case String:
		return &proto.Value{Value: &proto.Value_StringValue{StringValue: string(v)}}
	case Bool:
		return &proto.Value{Value: &proto.Value_BoolValue{BoolValue: bool(v)}}
	case Timestamp:
		return &proto.Value{Value: &proto.Value_TimestampValue{TimestampValue: toProtoTimestamp(v)}}
	case Array:
		return &proto.Value{Value: &proto.Value_ArrayValue{ArrayValue: toProtoArray(v)}}
	case Map:
		return &proto.Value{Value: &proto.Value_MapValue{MapValue: toProtoMap(v)}}
	default:
		// this should never happen as all types are covered
		log.Println("schema.toProtoValue found unknown type which should never happen")
		return &proto.Value{Value: &proto.Value_IntValue{IntValue: 0}}
	}
}

func fromProtoTimestamp(pts *proto.Timestamp) Timestamp {
	return Timestamp{
		Seconds: pts.Seconds,
		Nanos:   pts.Nanos,
		Now:     pts.Now,
	}
}

func toProtoTimestamp(ts Timestamp) *proto.Timestamp {
	return &proto.Timestamp{
		Seconds: ts.Seconds,
		Nanos:   ts.Nanos,
		Now:     ts.Now,
	}
}

func fromProtoArray(pa *proto.Array) Array {
	vals := make([]Value, len(pa.Elements))
	for i := range vals {
		vals[i] = fromProtoValue(pa.Elements[i])
	}
	return Array{Elements: vals}
}

func toProtoArray(a Array) *proto.Array {
	elems := make([]*proto.Value, len(a.Elements))
	for i := range elems {
		elems[i] = toProtoValue(a.Elements[i])
	}
	return &proto.Array{Elements: elems}
}

func fromProtoMap(pm *proto.Map) Map {
	keys := make([]Value, len(pm.Keys))
	vals := make([]Value, len(pm.Values))
	for i := range keys {
		keys[i] = fromProtoValue(pm.Keys[i])
		vals[i] = fromProtoValue(pm.Values[i])
	}
	return Map{
		Keys:   keys,
		Values: vals,
	}
}

func toProtoMap(m Map) *proto.Map {
	keys := make([]*proto.Value, len(m.Keys))
	vals := make([]*proto.Value, len(m.Values))
	for i := range keys {
		keys[i] = toProtoValue(m.Keys[i])
		vals[i] = toProtoValue(m.Values[i])
	}
	return &proto.Map{
		Keys:   keys,
		Values: vals,
	}
}
