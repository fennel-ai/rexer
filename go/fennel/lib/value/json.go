package value

import (
	"fmt"

	"github.com/buger/jsonparser"
)

func Clean(v Value) Value {
	switch v := v.(type) {
	case List:
		if v.values == nil {
			return NewList()
		}
	case Dict:
		if v.values == nil {
			return NewDict(nil)
		}
	}
	return v
}

func FromJSON(data []byte) (Value, error) {
	vdata, vtype, _, err := jsonparser.Get(data)
	if err != nil {
		fmt.Println(string(data))
		fmt.Println("DAMN ", err)
		return nil, err
	}
	return ParseJSON(vdata, vtype)
}

func ToJSON(val Value) []byte {
	if val == nil {
		return []byte("null")
	}
	return []byte(val.String())
}

func ParseJSON(vdata []byte, vtype jsonparser.ValueType) (Value, error) {
	switch vtype {
	case jsonparser.Boolean:
		return parseJSONBoolean(vdata)
	case jsonparser.Number:
		return parseJSONNumber(vdata)
	case jsonparser.String:
		return parseJSONString(vdata)
	case jsonparser.Array:
		return parseJSONArray(vdata)
	case jsonparser.Object:
		return parseJSONObject(vdata)
	case jsonparser.Null:
		return Nil, nil
	default:
		return nil, fmt.Errorf("unsupported json type %v", vtype)
		return nil, fmt.Errorf("unknown type")
	}
}

func parseJSONBoolean(data []byte) (Value, error) {
	if v, err := jsonparser.ParseBoolean(data); err != nil {
		return nil, err
	} else {
		return Bool(v), nil
	}
}

func parseJSONNumber(data []byte) (Value, error) {
	for i := 0; i < len(data); i++ {
		if data[i] == '.' {
			v, err := jsonparser.ParseFloat(data)
			return Double(v), err
		}
	}
	v, err := jsonparser.ParseInt(data)
	return Int(v), err
}

func parseJSONString(data []byte) (Value, error) {
	if v, err := jsonparser.ParseString(data); err != nil {
		return nil, err
	} else {
		return String(v), nil
	}
}

func parseJSONArray(data []byte) (Value, error) {
	var ret List
	var errors []error
	handler := func(vdata []byte, vtype jsonparser.ValueType, _ int, err error) {
		if err != nil {
			errors = append(errors, err)
			return
		}
		v, err := ParseJSON(vdata, vtype)
		if err != nil {
			errors = append(errors, err)
			return
		}
		ret.Append(v)
		//ret = append(ret, v)
	}
	_, err := jsonparser.ArrayEach(data, handler)
	if err != nil {
		return nil, err
	}
	if len(errors) != 0 {
		// should this combine errors instead of returning only first error?
		return nil, errors[0]
	}
	return ret, nil
}

func parseJSONObject(data []byte) (Value, error) {
	ret := NewDict(map[string]Value{})
	handler := func(key []byte, vdata []byte, vtype jsonparser.ValueType, _ int) error {
		k, err := jsonparser.ParseString(key)
		if err != nil {
			return err
		}
		v, err := ParseJSON(vdata, vtype)
		if err != nil {
			return err
		}
		ret.Set(k, v)
		return nil
	}
	err := jsonparser.ObjectEach(data, handler)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
