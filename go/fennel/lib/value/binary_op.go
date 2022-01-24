package value

import "fmt"

func route(l Value, opt string, other Value) (Value, error) {
	switch opt {
	case "+":
		return add(l, other)
	case "-":
		return sub(l, other)
	case "*":
		return mul(l, other)
	case "/":
		return div(l, other)
	case "==":
		return eq(l, other)
	case "!=":
		return neq(l, other)
	case ">=":
		return gte(l, other)
	case ">":
		return gt(l, other)
	case "<=":
		return lte(l, other)
	case "<":
		return lt(l, other)
	case "and":
		return and(l, other)
	case "or":
		return or(l, other)
	case "[]":
		return index(l, other)
	}
	return Nil, nil
}

// TODO: implement add for string and lists
func add(left Value, right Value) (Value, error) {
	switch left.(type) {
	case Int:
		switch right.(type) {
		case Int:
			return Int(int(left.(Int)) + int(right.(Int))), nil
		case Double:
			return Double(float64(left.(Int)) + float64(right.(Double))), nil

		}
	case Double:
		switch right.(type) {
		case Int:
			return Double(float64(left.(Double)) + float64(right.(Int))), nil
		case Double:
			return Double(float64(left.(Double)) + float64(right.(Double))), nil
		}
	}
	return nil, fmt.Errorf("'+' only supported between numbers")
}

func sub(left Value, right Value) (Value, error) {
	switch left.(type) {
	case Int:
		switch right.(type) {
		case Int:
			return Int(int(left.(Int)) - int(right.(Int))), nil
		case Double:
			return Double(float64(left.(Int)) - float64(right.(Double))), nil
		}
	case Double:
		switch right.(type) {
		case Int:
			return Double(float64(left.(Double)) - float64(right.(Int))), nil
		case Double:
			return Double(float64(left.(Double)) - float64(right.(Double))), nil
		}
	}
	return nil, fmt.Errorf("'+' only supported between numbers")
}

func div(left Value, right Value) (Value, error) {

	switch left.(type) {
	case Int:
		switch right.(type) {
		case Int:
			return Double(float64(left.(Int)) / float64(right.(Int))), nil
		case Double:
			return Double(float64(left.(Int)) / float64(right.(Double))), nil
		}
	case Double:
		switch right.(type) {
		case Int:
			return Double(float64(left.(Double)) / float64(right.(Int))), nil
		case Double:
			return Double(float64(left.(Double)) / float64(right.(Double))), nil
		}
	}
	return nil, fmt.Errorf("'/' only supported between numbers")
}

func mul(left Value, right Value) (Value, error) {

	switch left.(type) {
	case Int:
		switch right.(type) {
		case Int:
			return Int(int(left.(Int)) * int(right.(Int))), nil
		case Double:
			return Double(float64(left.(Int)) * float64(right.(Double))), nil
		}
	case Double:
		switch right.(type) {
		case Int:
			return Double(float64(left.(Double)) * float64(right.(Int))), nil
		case Double:
			return Double(float64(left.(Double)) * float64(right.(Double))), nil
		}
	}
	return nil, fmt.Errorf("'*' only supported between numbers")
}

func eq(left Value, right Value) (Value, error) {
	return Bool(left.Equal(right)), nil
}

func neq(left Value, right Value) (Value, error) {
	return Bool(!left.Equal(right)), nil
}

func or(left Value, right Value) (Value, error) {
	switch left.(type) {
	case Bool:
		switch right.(type) {
		case Bool:
			return Bool(bool(left.(Bool)) || bool(right.(Bool))), nil
		}
	}
	return nil, fmt.Errorf("'or' only supported between numbers")
}

func and(left Value, right Value) (Value, error) {
	switch left.(type) {
	case Bool:
		switch right.(type) {
		case Bool:
			return Bool(bool(left.(Bool)) && bool(right.(Bool))), nil
		}
	}
	return nil, fmt.Errorf("'and' only supported between numbers")
}

func lt(left Value, right Value) (Value, error) {
	switch left.(type) {
	case Int:
		switch right.(type) {
		case Int:
			return Bool(int(left.(Int)) < int(right.(Int))), nil
		case Double:
			return Bool(float64(left.(Int)) < float64(right.(Double))), nil
		}
	case Double:
		switch right.(type) {
		case Int:
			return Bool(float64(left.(Double)) < float64(right.(Int))), nil
		case Double:
			return Bool(float64(left.(Double)) < float64(right.(Double))), nil
		}
	}
	return nil, fmt.Errorf("'<' only supported between numbers")
}

func lte(left Value, right Value) (Value, error) {
	switch left.(type) {
	case Int:
		switch right.(type) {
		case Int:
			return Bool(int(left.(Int)) <= int(right.(Int))), nil
		case Double:
			return Bool(float64(left.(Int)) <= float64(right.(Double))), nil
		}
	case Double:
		switch right.(type) {
		case Int:
			return Bool(float64(left.(Double)) <= float64(right.(Int))), nil
		case Double:
			return Bool(float64(left.(Double)) <= float64(right.(Double))), nil
		}
	}
	return nil, fmt.Errorf("'<=' only supported between numbers")
}

func gt(left Value, right Value) (Value, error) {
	switch left.(type) {
	case Int:
		switch right.(type) {
		case Int:
			return Bool(int(left.(Int)) > int(right.(Int))), nil
		case Double:
			return Bool(float64(left.(Int)) > float64(right.(Double))), nil
		}
	case Double:
		switch right.(type) {
		case Int:
			return Bool(float64(left.(Double)) > float64(right.(Int))), nil
		case Double:
			return Bool(float64(left.(Double)) > float64(right.(Double))), nil
		}
	}
	return nil, fmt.Errorf("'>' only supported between numbers")
}

func gte(left Value, right Value) (Value, error) {
	switch left.(type) {
	case Int:
		switch right.(type) {
		case Int:
			return Bool(int(left.(Int)) >= int(right.(Int))), nil
		case Double:
			return Bool(float64(left.(Int)) >= float64(right.(Double))), nil
		}
	case Double:
		switch right.(type) {
		case Int:
			return Bool(float64(left.(Double)) >= float64(right.(Int))), nil
		case Double:
			return Bool(float64(left.(Double)) >= float64(right.(Double))), nil
		}
	}
	return nil, fmt.Errorf("'>=' only supported between numbers")
}

func index(left Value, right Value) (Value, error) {
	if asList, ok := left.(List); ok {
		asInt, ok := right.(Int)
		if !ok {
			return Nil, fmt.Errorf("can only index a list with int but got: '%s' instead", right)
		}
		idx := int(asInt)
		if idx < 0 || idx >= len(asList) {
			return Nil, fmt.Errorf("index out of bounds. Length of list: %d but index is: %d", len(asList), idx)
		}
		return left.(List)[idx], nil
	}
	if asDict, ok := left.(Dict); ok {
		asStr, ok := right.(String)
		if !ok {
			return Nil, fmt.Errorf("can only index a dict with string but got: '%s' instead", right)
		}
		idx := string(asStr)
		ret, ok := asDict[idx]
		if !ok {
			return Nil, fmt.Errorf("dict doesn't have property: %s", idx)
		}
		return ret, nil
	}
	return nil, fmt.Errorf("'index' operation supported only list or dict but got: '%T' instead", left)
}
