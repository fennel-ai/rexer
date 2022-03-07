package value

import (
	"encoding/json"
	"fmt"

	"github.com/buger/jsonparser"
)

// Clean takes a value, and returns a value with nil lists/dicts replaced by empty lists/dicts
func Clean(v Value) Value {
	switch v := v.(type) {
	case List:
		if v == nil {
			return List{}
		} else {
			cleanList(v)
		}
	case Dict:
		if v == nil {
			return Dict{}
		} else {
			cleanDict(v)
		}
	}
	return v
}

// cleanList recursively converts all nil lists/dicts in the list to empty lists/dicts
func cleanList(l List) {
	for i, e := range l {
		switch e := e.(type) {
		case List:
			if e == nil {
				l[i] = List{}
			} else {
				cleanList(e)
			}
		case Dict:
			if e == nil {
				l[i] = Dict{}
			} else {
				cleanDict(e)
			}
		}
	}
}

// cleanDict recursively converts all nil lists/dicts in the dict to empty lists/dicts
func cleanDict(d Dict) {
	for k, v := range d {
		switch v := v.(type) {
		case List:
			if v == nil {
				d[k] = List{}
			} else {
				cleanList(v)
			}
		case Dict:
			if v == nil {
				d[k] = Dict{}
			} else {
				cleanDict(v)
			}
		}
	}
}

func FromJSON(data []byte) (Value, error) {
	vdata, vtype, _, err := jsonparser.Get(data)
	if err != nil {
		return nil, err
	}
	return ParseJSON(vdata, vtype)
}

func ToJSON(val Value) ([]byte, error) {
	switch val := val.(type) {
	// When val is a nil list/dict, json.Marshal() marshals it as null. Marshal empty list/dict instead.
	// When val is a non-nul list/dict, recursively clean up elements which may be nil lists/dicts.
	case List:
		if val == nil {
			return json.Marshal(List{})
		} else {
			cleanList(val)
		}
	case Dict:
		if val == nil {
			return json.Marshal(Dict{})
		} else {
			cleanDict(val)
		}
	// When double is integral, json.Marshal() marshals it as an int.
	// Add ".0" at the end in that case.
	case Double:
		ser, err := json.Marshal(val)
		if err != nil {
			return nil, err
		}
		for _, b := range ser {
			if b == '.' {
				return ser, nil
			}
		}
		ser = append(ser, '.', '0')
		return ser, nil
	}
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
