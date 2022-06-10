package value

import (
	"fmt"
	"math"
)

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
	case "in":
		return contains(l, other)
	}
	return Nil, nil
}

func contains(e Value, iter Value) (Value, error) {
	switch t := iter.(type) {
	case List:
		for i := 0; i < t.Len(); i++ {
			v, _ := t.At(i)
			if v.Equal(e) {
				return Bool(true), nil
			}
		}
	case Dict:
		asstr, ok := e.(String)
		if !ok {
			return nil, fmt.Errorf("'in' operation on dicts can only be done using strings, but given: '%s'", e)
		}
		for k, _ := range t.Iter() {
			if k == string(asstr) {
				return Bool(true), nil
			}
		}
	default:
		return nil, fmt.Errorf("'in' operator is only defined on lists/dicts but called on: '%s'", iter)
	}
	return Bool(false), nil
}

func modulo(left Value, right Value) (Value, error) {
	lint, ok := left.(Int)
	if !ok {
		return Nil, fmt.Errorf("'%%' only supported between ints but got: '%s'", left.String())
	}
	rint, ok := right.(Int)
	if !ok {
		return Nil, fmt.Errorf("'%%' only supported between ints but got: '%s'", right.String())
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
			v := make([]Value, left.Len()+right.Len())
			for i, lval := range left.values {
				v[i] = lval.Clone()
			}
			for i, rval := range right.values {
				v[left.Len()+i] = rval.Clone()
			}
			return List{values: v}, nil
		}
	}
	return nil, fmt.Errorf("'+' only supported between numbers, strings and lists. Got '%s' and '%s'", left.String(), right.String())
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
	return nil, fmt.Errorf("'-' only supported between numbers. Got '%s' and '%s'", left.String(), right.String())
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
	return nil, fmt.Errorf("'/' only supported between numbers. Got '%s' and '%s'", left.String(), right.String())
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
	return nil, fmt.Errorf("'//' only supported between numbers. Got '%s' and '%s'", left.String(), right.String())

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
	return nil, fmt.Errorf("'*' only supported between numbers. Got '%s' and '%s'", left.String(), right.String())
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
	return nil, fmt.Errorf("'or' only supported between booleans. Got '%s' and '%s'", left.String(), right.String())
}

func and(left Value, right Value) (Value, error) {
	switch left := left.(type) {
	case Bool:
		switch right := right.(type) {
		case Bool:
			return Bool(bool(left) && bool(right)), nil
		}
	}
	return nil, fmt.Errorf("'and' only supported between booleans. Got '%s' and '%s'", left.String(), right.String())
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
	return nil, fmt.Errorf("'<' only supported between numbers. Got '%s' and '%s'", left.String(), right.String())
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
	return nil, fmt.Errorf("'<=' only supported between numbers. Got '%s' and '%s'", left.String(), right.String())
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
	return nil, fmt.Errorf("'>' only supported between numbers. Got '%s' and '%s'", left.String(), right.String())
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
	return nil, fmt.Errorf("'>=' only supported between numbers. Got '%s' and '%s'", left.String(), right.String())
}

func index(left Value, right Value) (Value, error) {
	if asList, ok := left.(List); ok {
		asInt, ok := right.(Int)
		if !ok {
			return Nil, fmt.Errorf("can only index a list with int but got: '%s' instead", right)
		}
		idx := int(asInt)
		return asList.At(idx)
	}
	if asDict, ok := left.(Dict); ok {
		asStr, ok := right.(String)
		if !ok {
			return Nil, fmt.Errorf("can only index a dict with string but got: '%s' instead", right)
		}
		idx := string(asStr)
		ret, ok := asDict.Get(idx)
		if !ok {
			return Nil, fmt.Errorf("dict doesn't have property: %s", idx)
		}
		return ret, nil
	}
	return nil, fmt.Errorf("'index' operation supported only on lists or dicts but got: '%T' instead", left)
}
