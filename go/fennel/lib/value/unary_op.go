package value

import "fmt"

func routeUnary(opt string, operand Value) (Value, error) {
	switch opt {
	case "~":
		return not(operand)
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
