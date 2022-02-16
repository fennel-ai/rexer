package value

import (
	"encoding/json"
	"fmt"
	"github.com/buger/jsonparser"
)

func FromJSON(data []byte) (Value, error) {
	vdata, vtype, _, err := jsonparser.Get(data)
	if err != nil {
		return nil, err
	}
	return ParseJSON(vdata, vtype)
}

func ToJSON(val Value) ([]byte, error) {
	return json.Marshal(val)
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
	vFloat, err := jsonparser.ParseFloat(data)
	if err != nil {
		return nil, err
	}
	vInt, err := jsonparser.ParseInt(data)
	if err != nil {
		return Double(vFloat), nil
	} else {
		return Int(vInt), nil
	}
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
		ret = append(ret, v)
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
	ret := make(Dict)
	handler := func(key []byte, vdata []byte, vtype jsonparser.ValueType, _ int) error {
		k, err := jsonparser.ParseString(key)
		if err != nil {
			return err
		}
		v, err := ParseJSON(vdata, vtype)
		if err != nil {
			return err
		}
		ret[k] = v
		return nil
	}
	err := jsonparser.ObjectEach(data, handler)
	if err != nil {
		return nil, err
	}
	return ret, nil
}
