package value

import (
	"fmt"
	"math"
)

func route(l Value, opt string, other Value) (Value, error) {
	if f, ok := other.(*Future); ok {
		other = f.Await()
	}
	switch opt {
	case "+":
		return add(l, other)
	case "-":
		return sub(l, other)
	case "*":
		return mul(l, other)
	case "/":
		return div(l, other)
	case "//":
		return fdiv(l, other)
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
	case "%":
		return modulo(l, other)
	}
	return Nil, nil
}

func modulo(left Value, right Value) (Value, error) {
	lint, ok := left.(Int)
	if !ok {
		return Nil, fmt.Errorf("'%%' only supported between ints but got: '%v'", left)
	}
	rint, ok := right.(Int)
	if !ok {
		return Nil, fmt.Errorf("'%%' only supported between ints but got: '%v'", right)
	}
	if rint == 0 {
		return Nil, fmt.Errorf("division by zero while using '%%'")
	}
	return lint % rint, nil
}

func add(left Value, right Value) (Value, error) {
	switch left := left.(type) {
	case Int:
		switch right := right.(type) {
		case Int:
			return Int(int(left) + int(right)), nil
		case Double:
			return Double(float64(left) + float64(right)), nil

		}
	case Double:
		switch right := right.(type) {
		case Int:
			return Double(float64(left) + float64(right)), nil
		case Double:
			return Double(float64(left) + float64(right)), nil
		}
	case String:
		switch right := right.(type) {
		case String:
			return String(string(left) + string(right)), nil
		}
	case List:
		switch right := right.(type) {
		case List:
			v := make([]Value, len(left)+len(right))
			for i, lval := range left {
				v[i] = lval.Clone()
			}
			for i, rval := range right {
				v[len(left)+i] = rval.Clone()
			}
			return List(v), nil
		}
	}
	return nil, fmt.Errorf("'+' only supported between numbers, strings and lists")
}

func sub(left Value, right Value) (Value, error) {
	switch left := left.(type) {
	case Int:
		switch right := right.(type) {
		case Int:
			return Int(int(left) - int(right)), nil
		case Double:
			return Double(float64(left) - float64(right)), nil
		}
	case Double:
		switch right := right.(type) {
		case Int:
			return Double(float64(left) - float64(right)), nil
		case Double:
			return Double(float64(left) - float64(right)), nil
		}
	}
	return nil, fmt.Errorf("'+' only supported between numbers")
}

func div(left Value, right Value) (Value, error) {
	if right.Equal(Int(0)) || right.Equal(Double(0)) {
		return Nil, fmt.Errorf("division by zero while using '/'")
	}

	switch left := left.(type) {
	case Int:
		switch right := right.(type) {
		case Int:
			return Double(float64(left) / float64(right)), nil
		case Double:
			return Double(float64(left) / float64(right)), nil
		}
	case Double:
		switch right := right.(type) {
		case Int:
			return Double(float64(left) / float64(right)), nil
		case Double:
			return Double(float64(left) / float64(right)), nil
		}
	}
	return nil, fmt.Errorf("'/' only supported between numbers")
}

func fdiv(left Value, right Value) (Value, error) {
	if right.Equal(Int(0)) || right.Equal(Double(0)) {
		return Nil, fmt.Errorf("division by zero while using '//'")
	}

	switch left := left.(type) {
	case Int:
		switch right := right.(type) {
		case Int:
			return Int(math.Floor(float64(left) / float64(right))), nil
		case Double:
			return Double(math.Floor(float64(left) / float64(right))), nil
		}
	case Double:
		switch right := right.(type) {
		case Int:
			return Double(math.Floor(float64(left) / float64(right))), nil
		case Double:
			return Double(math.Floor(float64(left) / float64(right))), nil
		}
	}
	return nil, fmt.Errorf("'//' only supported between numbers")

}

func mul(left Value, right Value) (Value, error) {
	switch left := left.(type) {
	case Int:
		switch right := right.(type) {
		case Int:
			return Int(int(left) * int(right)), nil
		case Double:
			return Double(float64(left) * float64(right)), nil
		}
	case Double:
		switch right := right.(type) {
		case Int:
			return Double(float64(left) * float64(right)), nil
		case Double:
			return Double(float64(left) * float64(right)), nil
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
	switch left := left.(type) {
	case Bool:
		switch right := right.(type) {
		case Bool:
			return Bool(bool(left) || bool(right)), nil
		}
	}
	return nil, fmt.Errorf("'or' only supported between numbers")
}

func and(left Value, right Value) (Value, error) {
	switch left := left.(type) {
	case Bool:
		switch right := right.(type) {
		case Bool:
			return Bool(bool(left) && bool(right)), nil
		}
	}
	return nil, fmt.Errorf("'and' only supported between numbers")
}

func lt(left Value, right Value) (Value, error) {
	switch left := left.(type) {
	case Int:
		switch right := right.(type) {
		case Int:
			return Bool(int(left) < int(right)), nil
		case Double:
			return Bool(float64(left) < float64(right)), nil
		}
	case Double:
		switch right := right.(type) {
		case Int:
			return Bool(float64(left) < float64(right)), nil
		case Double:
			return Bool(float64(left) < float64(right)), nil
		}
	}
	return nil, fmt.Errorf("'<' only supported between numbers")
}

func lte(left Value, right Value) (Value, error) {
	switch left := left.(type) {
	case Int:
		switch right := right.(type) {
		case Int:
			return Bool(int(left) <= int(right)), nil
		case Double:
			return Bool(float64(left) <= float64(right)), nil
		}
	case Double:
		switch right := right.(type) {
		case Int:
			return Bool(float64(left) <= float64(right)), nil
		case Double:
			return Bool(float64(left) <= float64(right)), nil
		}
	}
	return nil, fmt.Errorf("'<=' only supported between numbers")
}

func gt(left Value, right Value) (Value, error) {
	switch left := left.(type) {
	case Int:
		switch right := right.(type) {
		case Int:
			return Bool(int(left) > int(right)), nil
		case Double:
			return Bool(float64(left) > float64(right)), nil
		}
	case Double:
		switch right := right.(type) {
		case Int:
			return Bool(float64(left) > float64(right)), nil
		case Double:
			return Bool(float64(left) > float64(right)), nil
		}
	}
	return nil, fmt.Errorf("'>' only supported between numbers")
}

func gte(left Value, right Value) (Value, error) {
	switch left := left.(type) {
	case Int:
		switch right := right.(type) {
		case Int:
			return Bool(int(left) >= int(right)), nil
		case Double:
			return Bool(float64(left) >= float64(right)), nil
		}
	case Double:
		switch right := right.(type) {
		case Int:
			return Bool(float64(left) >= float64(right)), nil
		case Double:
			return Bool(float64(left) >= float64(right)), nil
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
	return nil, fmt.Errorf("'index' operation supported only on lists or dicts but got: '%T' instead", left)
}
