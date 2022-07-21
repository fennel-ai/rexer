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
	case "^":
		return power(l, other)
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
		for k := range t.Iter() {
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
	case Dict:
		// Ideally we shouldn't be merging dictionaries in the 'or' binary operator, but as rql is not
		// type aware and also given that python's merge ('|') operator resolves internally to __or__()
		// method in an object it becomes hard to resolve when to send '|' vs 'or' to rexer.
		// Eg.
		// Suppose a user does the following.
		// a = Dict(a=1,b=2) | Dict(a=2) | Dict(b=3)
		// The first two Dict with | would form a Binary expression and then the it would be Binary | Dict
		// Because, the Binary expression doesn't know the type of the expression (this is what I meant by lack
		// of type awareness in RQL) we wouldn't want to know how to overload the __or__() method.
		// BTW, since Dict and List ctrs are not supposed to be private this might not be the right way of using.
		// The typical way a user would be using the merge operator would be the following.
		// op.std.map([{'a':1, 'b':2}], [{'a': 3}], var=['a', 'b'], to=var('a') | var('b'))
		// Again, here the rql doesn't know the type of var('a') and var('b') in to expression so it would simply
		// always resolve it to 'or' operator.
		// Hence, we are abusing the or operator here to also work between dictionaries and perform a merge with
		// last dictionary holds the final value incase of key matches (what a python merge operator does).
		switch right := right.(type) {
		case Dict:
			return left.Merge(right), nil
		}
	}
	return nil, fmt.Errorf("'or' only supported between booleans or between dictionaries. Got '%s' and '%s'", left.String(), right.String())
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

func power(left Value, right Value) (Value, error) {
	switch left := left.(type) {
	case Int:
		switch right := right.(type) {
		case Int:
			return Int(math.Pow(float64(left), float64(right))), nil
		case Double:
			return Double(math.Pow(float64(left), float64(right))), nil
		}
	case Double:
		switch right := right.(type) {
		case Int:
			return Double(math.Pow(float64(left), float64(right))), nil
		case Double:
			return Double(math.Pow(float64(left), float64(right))), nil
		}
	}
	return nil, fmt.Errorf("'^' only supported between numbers. Got '%s' and '%s'", left.String(), right.String())
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
