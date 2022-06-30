package value

import (
	"fennel/lib/value/rexparser"
	"fmt"
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
	vdata, vtype, _, sz, err := rexparser.Get(data)
	if err != nil {
		return nil, fmt.Errorf("failed to create parser: %w", err)
	}
	//fmt.Println(vtype)
	return ParseJSON(vdata, vtype, sz)
}

func ToJSON(val Value) []byte {
	if val == nil {
		return []byte("null")
	}
	return []byte(val.String())
}

func ParseJSON(vdata []byte, vtype rexparser.ValueType, sz int) (Value, error) {
	switch vtype {
	case rexparser.Boolean:
		return parseJSONBoolean(vdata)
	case rexparser.Number:
		return parseJSONNumber(vdata)
	case rexparser.RString:
		return parseJSONString(vdata)
	case rexparser.Array:
		return parseJSONArray(vdata, sz)
	case rexparser.Object:
		return parseJSONObject(vdata, sz)
	case rexparser.Null:
		return Nil, nil
	default:
		return nil, fmt.Errorf("unsupported json type %v", vtype)
	}
}

func parseJSONBoolean(data []byte) (Value, error) {
	if v, err := rexparser.ParseBoolean(data); err != nil {
		return nil, err
	} else {
		return Bool(v), nil
	}
}

func parseJSONNumber(data []byte) (Value, error) {
	for i := 0; i < len(data); i++ {
		if data[i] == '.' {
			v, err := rexparser.ParseFloat(data)
			return Double(v), err
		}
	}
	v, err := rexparser.ParseInt(data)
	return Int(v), err
}

func parseJSONString(data []byte) (Value, error) {
	if v, err := rexparser.ParseString(data); err != nil {
		return nil, err
	} else {
		return String(v), nil
	}
}

func parseJSONArray(data []byte, sz int) (Value, error) {
	//start := time.Now()
	var ret List
	ret.Grow(sz)

	//fmt.Println(string(data))
	var errors []error
	handler := func(vdata []byte, vtype rexparser.ValueType, _ int, sz int, err error) {
		//start := time.Now()
		if err != nil {
			errors = append(errors, err)
			return
		}
		v, err := ParseJSON(vdata, vtype, sz)
		if err != nil {
			errors = append(errors, err)
			return
		}
		//fmt.Println("Time  spent: ", time.Since(start))
		ret.Append(v)
		//ret = append(ret, v)
	}
	_, err := rexparser.ArrayEach(data, handler)
	if err != nil {
		return nil, err
	}
	if len(errors) != 0 {
		// should this combine errors instead of returning only first error?
		return nil, errors[0]
	}
	//fmt.Println("Elapsed Array: ", time.Since(start))
	return ret, nil
}

func parseJSONObject(data []byte, sz int) (Value, error) {
	//start := time.Now()
	//ret := NewDict(map[string]Value{})
	ret := make(map[string]Value, sz)
	handler := func(key []byte, vdata []byte, vtype rexparser.ValueType, _ int, sz int) error {
		k, err := rexparser.ParseString(key)
		if err != nil {
			return err
		}
		v, err := ParseJSON(vdata, vtype, sz)
		if err != nil {
			return err
		}
		//ret.Set(k, v)
		ret[k] = v
		return nil
	}
	// rexparser.EachKey(data, handler, nil)
	err := rexparser.ObjectEach(data, handler)
	if err != nil {
		return nil, err
	}
	//fmt.Println("Elapsed Dict: ", time.Since(start))

	return NewDict(ret), nil
}
