package runtime

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
