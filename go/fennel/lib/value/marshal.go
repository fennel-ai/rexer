package value

import (
	"bytes"
	"capnproto.org/go/capnp/v3"
	"fennel/lib/utils/binary"
	"fennel/lib/value/rexparser"
	"fmt"
	"google.golang.org/protobuf/proto"
)

func CaptainMarshal(v Value) ([]byte, error) {
	_, bytes, err := ToCapnValue(v)
	return bytes, err
}

func CaptainUnmarshal(data []byte) (Value, error) {
	msg, _ := capnp.NewDecoder(bytes.NewBuffer(data)).Decode()
	cv, err := ReadRootCapnValue(msg)
	if err != nil {
		return nil, err
	}
	v, err := FromCapnValue(cv)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func ProtoMarshal(v Value) ([]byte, error) {
	pa, err := ToProtoValue(v)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pa)
}

func ProtoUnmarshal(data []byte, v *Value) error {
	var pa PValue
	if err := proto.Unmarshal(data, &pa); err != nil {
		return err
	}
	a, err := FromProtoValue(&pa)
	if err != nil {
		return err
	}
	*v = a
	return nil
}

func Unmarshal(data []byte) (Value, error) {
	vdata, vtype, _, sz, err := rexparser.Get(data)
	if err != nil {
		return nil, fmt.Errorf("failed to create parser: %w", err)
	}
	return parseCustomJSON(vdata, vtype, sz)
}

func parseCustomJSON(vdata []byte, vtype rexparser.ValueType, sz int) (Value, error) {
	switch vtype {
	case rexparser.Boolean:
		return parseCustomJSONBoolean(vdata)
	case rexparser.Integer:
		return parseCustomJSONInteger(vdata)
	case rexparser.Float:
		return parseCustomJSONFloat(vdata)
	case rexparser.String:
		return ParseJSONString(vdata)
	case rexparser.Array:
		return parseCustomJSONArray(vdata, sz)
	case rexparser.Object:
		return parseCustomJSONObject(vdata, sz)
	case rexparser.Null:
		return Nil, nil
	default:
		return nil, fmt.Errorf("unsupported json type %v", vtype)
	}
}

func parseCustomJSONBoolean(data []byte) (Value, error) {
	if len(data) != 1 {
		return nil, fmt.Errorf("invalid boolean")
	}
	data[0] = data[0] & 0x3F
	if data[0] == 0 {
		return Bool(false), nil
	}
	return Bool(true), nil
}

func parseCustomJSONInteger(data []byte) (Value, error) {
	return Int(binary.ParseInteger(data)), nil
}

func parseCustomJSONFloat(data []byte) (Value, error) {
	return Double(binary.ParseFloat(data)), nil
}

func parseCustomJSONArray(data []byte, sz int) (Value, error) {
	var ret List
	ret.Grow(sz)
	var errors []error
	handler := func(vdata []byte, vtype rexparser.ValueType, _ int, sz int, err error) {
		if err != nil {
			errors = append(errors, err)
			return
		}
		v, err := parseCustomJSON(vdata, vtype, sz)
		if err != nil {
			errors = append(errors, err)
			return
		}
		ret.Append(v)
	}
	_, err := rexparser.ArrayEach(data, handler)
	if err != nil {
		return nil, err
	}
	if len(errors) != 0 {
		return nil, errors[0]
	}
	return ret, nil
}

func parseCustomJSONObject(data []byte, sz int) (Value, error) {
	ret := make(map[string]Value, sz)
	handler := func(key []byte, vdata []byte, vtype rexparser.ValueType, _ int, sz int) error {
		k, err := rexparser.ParseString(key)
		if err != nil {
			return err
		}
		v, err := parseCustomJSON(vdata, vtype, sz)
		if err != nil {
			return err
		}
		ret[k] = v
		return nil
	}
	err := rexparser.ObjectEach(data, handler)
	if err != nil {
		return nil, err
	}
	return NewDict(ret), nil
}
