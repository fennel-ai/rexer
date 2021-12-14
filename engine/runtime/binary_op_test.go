package runtime

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func verifyOp(t *testing.T, left, right, expected Value, op string) {
	ret, err := left.Op(op, right)
	if expected != nil {
		assert.NoError(t, err)
		assert.Equal(t, expected, ret)
	} else {
		assert.Error(t, err)
	}
}

func verifyError(t *testing.T, left, right Value, ops []string) {
	for _, op := range ops {
		_, err := left.Op(op, right)
		assert.Error(t, err)
	}
}

func TestInvalid(t *testing.T) {
	//i := Int(2)
	//d := Double(3.0)
	//b := Bool(false)
	//s := String("hi")
	//l := List([]Value{Int(1), Double(2.0), Bool(true)})
	//di := Dict(map[string]Value{"a": Int(2), "b": Double(1.0)})
	//n := Nil
	//ops := []string{"+", "-", "*", "/", ">", ">=", "<", "<=", "and", "or"}
	//
	//// add
	//verifyError(t, i, d, []string{"and", "or"})
	//verifyError(t, i, b, ops)
}

func TestValidArithmetic(t *testing.T) {
	// Add
	var base Value
	base = Int(1)
	verifyOp(t, Int(1), Int(2), Int(3), "+")
	verifyOp(t, Int(1), Double(2.0), Double(3.0), "+")
	base = Double(1.0)
	verifyOp(t, base, Int(2), Double(3.0), "+")
	verifyOp(t, base, Double(2.0), Double(3.0), "+")

	verifyOp(t, base, String("hi"), nil, "+")
	verifyOp(t, base, Bool(true), nil, "+")
	verifyOp(t, base, Nil, nil, "+")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "+")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "+")

	// Double
	base = Double(1.0)
	verifyOp(t, base, Int(2), Double(3.0), "+")
	verifyOp(t, base, Double(2.0), Double(3.0), "+")
	verifyOp(t, base, String("hi"), nil, "+")
	verifyOp(t, base, Bool(true), nil, "+")
	verifyOp(t, base, Nil, nil, "+")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "+")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "+")

	// String
	base = String("hi")
	verifyOp(t, base, Int(2), nil, "+")
	verifyOp(t, base, Double(2.0), nil, "+")
	verifyOp(t, base, String("hi"), nil, "+")
	verifyOp(t, base, Bool(true), nil, "+")
	verifyOp(t, base, Nil, nil, "+")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "+")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "+")
	// Bool
	base = Bool(true)
	verifyOp(t, base, Int(2), nil, "+")
	verifyOp(t, base, Double(2.0), nil, "+")
	verifyOp(t, base, String("hi"), nil, "+")
	verifyOp(t, base, Bool(true), nil, "+")
	verifyOp(t, base, Nil, nil, "+")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "+")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "+")

	// List
	base = List([]Value{Int(1), Double(2.0), Bool(true)})
	verifyOp(t, base, Int(2), nil, "+")
	verifyOp(t, base, Double(2.0), nil, "+")
	verifyOp(t, base, String("hi"), nil, "+")
	verifyOp(t, base, Bool(true), nil, "+")
	verifyOp(t, base, Nil, nil, "+")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "+")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "+")

	// Dict
	base = Dict(map[string]Value{"a": Int(2), "b": Double(1.0)})
	verifyOp(t, base, Int(2), nil, "+")
	verifyOp(t, base, Double(2.0), nil, "+")
	verifyOp(t, base, String("hi"), nil, "+")
	verifyOp(t, base, Bool(true), nil, "+")
	verifyOp(t, base, Nil, nil, "+")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "+")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "+")
}

func TestSub(t *testing.T) {
	// Int
	var base Value
	base = Int(1)
	verifyOp(t, base, Int(2), Int(-1), "-")
	verifyOp(t, base, Double(2.0), Double(-1.0), "-")
	verifyOp(t, base, String("hi"), nil, "-")
	verifyOp(t, base, Bool(true), nil, "-")
	verifyOp(t, base, Nil, nil, "-")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "-")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "-")

	// Double
	base = Double(1.0)
	verifyOp(t, base, Int(2), Double(-1.0), "-")
	verifyOp(t, base, Double(2.0), Double(-1.0), "-")
	verifyOp(t, base, String("hi"), nil, "-")
	verifyOp(t, base, Bool(true), nil, "-")
	verifyOp(t, base, Nil, nil, "-")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "-")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "-")

	// String
	base = String("hi")
	verifyOp(t, base, Int(2), nil, "-")
	verifyOp(t, base, Double(2.0), nil, "-")
	verifyOp(t, base, String("hi"), nil, "-")
	verifyOp(t, base, Bool(true), nil, "-")
	verifyOp(t, base, Nil, nil, "-")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "-")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "-")
	// Bool
	base = Bool(true)
	verifyOp(t, base, Int(2), nil, "-")
	verifyOp(t, base, Double(2.0), nil, "-")
	verifyOp(t, base, String("hi"), nil, "-")
	verifyOp(t, base, Bool(true), nil, "-")
	verifyOp(t, base, Nil, nil, "-")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "-")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "-")

	// List
	base = List([]Value{Int(1), Double(2.0), Bool(true)})
	verifyOp(t, base, Int(2), nil, "-")
	verifyOp(t, base, Double(2.0), nil, "-")
	verifyOp(t, base, String("hi"), nil, "-")
	verifyOp(t, base, Bool(true), nil, "-")
	verifyOp(t, base, Nil, nil, "-")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "-")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "-")

	// Dict
	base = Dict(map[string]Value{"a": Int(2), "b": Double(1.0)})
	verifyOp(t, base, Int(2), nil, "-")
	verifyOp(t, base, Double(2.0), nil, "-")
	verifyOp(t, base, String("hi"), nil, "-")
	verifyOp(t, base, Bool(true), nil, "-")
	verifyOp(t, base, Nil, nil, "-")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "-")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "-")
}

func TestMul(t *testing.T) {
	// Int
	var base Value
	base = Int(2)
	verifyOp(t, base, Int(2), Int(4), "*")
	verifyOp(t, base, Double(2.0), Double(4.0), "*")
	verifyOp(t, base, String("hi"), nil, "*")
	verifyOp(t, base, Bool(true), nil, "*")
	verifyOp(t, base, Nil, nil, "*")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "*")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "*")

	// Double
	base = Double(2.0)
	verifyOp(t, base, Int(2), Double(4.0), "*")
	verifyOp(t, base, Double(2.0), Double(4.0), "*")
	verifyOp(t, base, String("hi"), nil, "*")
	verifyOp(t, base, Bool(true), nil, "*")
	verifyOp(t, base, Nil, nil, "*")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "*")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "*")

	// String
	base = String("hi")
	verifyOp(t, base, Int(2), nil, "*")
	verifyOp(t, base, Double(2.0), nil, "*")
	verifyOp(t, base, String("hi"), nil, "*")
	verifyOp(t, base, Bool(true), nil, "*")
	verifyOp(t, base, Nil, nil, "*")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "*")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "*")
	// Bool
	base = Bool(true)
	verifyOp(t, base, Int(2), nil, "*")
	verifyOp(t, base, Double(2.0), nil, "*")
	verifyOp(t, base, String("hi"), nil, "*")
	verifyOp(t, base, Bool(true), nil, "*")
	verifyOp(t, base, Nil, nil, "*")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "*")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "*")

	// List
	base = List([]Value{Int(1), Double(2.0), Bool(true)})
	verifyOp(t, base, Int(2), nil, "*")
	verifyOp(t, base, Double(2.0), nil, "*")
	verifyOp(t, base, String("hi"), nil, "*")
	verifyOp(t, base, Bool(true), nil, "*")
	verifyOp(t, base, Nil, nil, "*")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "*")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "*")

	// Dict
	base = Dict(map[string]Value{"a": Int(2), "b": Double(1.0)})
	verifyOp(t, base, Int(2), nil, "*")
	verifyOp(t, base, Double(2.0), nil, "*")
	verifyOp(t, base, String("hi"), nil, "*")
	verifyOp(t, base, Bool(true), nil, "*")
	verifyOp(t, base, Nil, nil, "*")
	verifyOp(t, base, List([]Value{Int(2), Double(1.0)}), nil, "*")
	verifyOp(t, base, Dict(map[string]Value{"a": Int(2), "b": Double(1.0)}), nil, "*")
}

func TestDiv(t *testing.T) {
}
