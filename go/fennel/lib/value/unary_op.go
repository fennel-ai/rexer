package value

import "fmt"

func routeUnary(opt string, operand Value) (Value, error) {
	switch opt {
	case "~":
		return not(operand)
	case "len":
		return len_(operand)
	case "str":
		return str(operand)
	}
	return Nil, nil
}

func not(v Value) (Value, error) {
	switch v := v.(type) {
	case Bool:
		return !v, nil
	}
	return nil, fmt.Errorf("'!' only supported on booleans")
}

func len_(v Value) (Value, error) {
	switch v := v.(type) {
	case List:
		return Int(v.Len()), nil
	case Dict:
		return Int(v.Len()), nil
	}
	return nil, fmt.Errorf("'len' only supported on booleans")
}

func str(v Value) (Value, error) {
	return String(v.String()), nil
}
