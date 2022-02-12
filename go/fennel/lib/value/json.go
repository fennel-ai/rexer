package value

import (
	"encoding/json"
	"fmt"
	"github.com/buger/jsonparser"
)

func FromJson(data []byte) (Value, error) {
	vdata, vtype, _, err := jsonparser.Get(data)
	if err != nil {
		return nil, err
	}
	val, err := parseJson(vdata, vtype)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func ToJson(val Value) ([]byte, error) {
	switch val.(type) {
	case nil_:
		data, err := json.Marshal(nil)
		if err != nil {
			return nil, err
		}
		return data, nil
	case Bool, Int, Double, String, List, Dict:
		data, err := json.Marshal(val)
		if err != nil {
			return nil, err
		}
		return data, nil
	default:
		return nil, fmt.Errorf("json serialization for %T not implemented", val)
	}
}

func parseJson(vdata []byte, vtype jsonparser.ValueType) (Value, error) {
	switch vtype {
	case jsonparser.Boolean:
		if v, err := jsonparser.ParseBoolean(vdata); err == nil {
			return Bool(v), nil
		} else {
			return nil, err
		}
	case jsonparser.Number:
		if v, err := jsonparser.ParseInt(vdata); err == nil {
			return Int(v), nil
		} else if v, err := jsonparser.ParseFloat(vdata); err == nil {
			return Double(v), nil
		} else {
			return nil, err
		}
	case jsonparser.String:
		if v, err := jsonparser.ParseString(vdata); err == nil {
			return String(v), nil
		} else {
			return nil, err
		}
	case jsonparser.Array:
		var ret List
		var errors []error
		handler := func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
			if err != nil {
				errors = append(errors, err)
			}
			v, err := parseJson(value, dataType)
			if err != nil {
				errors = append(errors, err)
			} else {
				ret = append(ret, v)
			}
		}
		_, err := jsonparser.ArrayEach(vdata, handler)
		if err != nil {
			return nil, err
		}
		if len(errors) != 0 {
			// TODO: Maybe combine errors instead of returning only first error
			return nil, errors[0]
		}
		return ret, nil
	case jsonparser.Object:
		ret := make(Dict)
		handler := func(key []byte, value []byte, dataType jsonparser.ValueType, offset int) error {
			k, err := jsonparser.ParseString(key)
			if err != nil {
				return err
			}
			v, err := parseJson(value, dataType)
			if err != nil {
				return err
			}
			ret[k] = v
			return nil
		}
		err := jsonparser.ObjectEach(vdata, handler)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case jsonparser.Null:
		return Nil, nil
	default:
		return nil, fmt.Errorf("unknown type")
	}
}
