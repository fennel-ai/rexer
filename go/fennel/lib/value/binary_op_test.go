package value

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func verifyOp(t *testing.T, left, right, expected Value, op string) {
	ret, err := left.Op(op, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, ret)
}

func verifyError(t *testing.T, left, right Value, ops []string) {
	for _, op := range ops {
		_, err := left.Op(op, right)
		assert.Error(t, err)
	}
}

func TestInvalid(t *testing.T) {
	i := Int(2)
	d := Double(3.0)
	b := Bool(false)
	s := String("hi")
	l := List([]Value{Int(1), Double(2.0), Bool(true)})
	di := Dict(map[string]Value{"a": Int(2), "b": Double(1.0)})
	n := Nil
	ops := []string{"+", "-", "*", "/", ">", ">=", "<", "<=", "and", "or", "[]", "%"}

	// ints with others
	verifyError(t, i, d, []string{"and", "or", "[]", "%"})
	verifyError(t, i, b, ops)
	verifyError(t, i, s, ops)
	verifyError(t, i, l, ops)
	verifyError(t, i, di, ops)
	verifyError(t, i, n, ops)
	// and div/modulo throws an error when denominator is zero
	verifyError(t, i, Int(0), []string{"%", "/"})
	verifyError(t, i, Double(0), []string{"%", "/"})

	verifyError(t, d, i, []string{"and", "or", "%", "[]"})
	verifyError(t, d, b, ops)
	verifyError(t, d, s, ops)
	verifyError(t, d, l, ops)
	verifyError(t, d, di, ops)
	verifyError(t, d, n, ops)
	// and div throws an error when denominator is zero
	verifyError(t, d, Int(0), []string{"/"})
	verifyError(t, d, Double(0), []string{"/"})

	verifyError(t, b, i, ops)
	verifyError(t, b, b, []string{"+", "-", "*", "/", ">", ">=", "<", "<=", "[]", "%"})
	verifyError(t, b, s, ops)
	verifyError(t, b, l, ops)
	verifyError(t, b, di, ops)
	verifyError(t, b, n, ops)

	verifyError(t, s, i, ops)
	verifyError(t, s, b, ops)
	verifyError(t, s, s, ops)
	verifyError(t, s, l, ops)
	verifyError(t, s, di, ops)
	verifyError(t, s, n, ops)

	// can only do indexing using a list and an int
	verifyError(t, l, i, []string{"+", "-", "*", "/", ">", ">=", "<", "<=", "and", "or", "%"})
	verifyError(t, l, b, ops)
	verifyError(t, l, s, ops)
	verifyError(t, l, l, ops)
	verifyError(t, l, di, ops)
	verifyError(t, l, n, ops)

	verifyError(t, di, i, ops)
	verifyError(t, di, b, ops)
	// can only do an indexing on dictionary using a string
	verifyError(t, di, s, []string{"+", "-", "*", "/", ">", ">=", "<", "<=", "and", "or", "%"})
	verifyError(t, di, l, ops)
	verifyError(t, di, di, ops)
	verifyError(t, di, n, ops)

	verifyError(t, n, i, ops)
	verifyError(t, n, b, ops)
	verifyError(t, n, s, ops)
	verifyError(t, n, l, ops)
	verifyError(t, n, di, ops)
	verifyError(t, n, n, ops)
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

	// Sub
	base = Int(1)
	verifyOp(t, Int(1), Int(2), Int(-1), "-")
	verifyOp(t, Int(1), Double(2.0), Double(-1.0), "-")
	base = Double(1.0)
	verifyOp(t, base, Int(2), Double(-1.0), "-")
	verifyOp(t, base, Double(2.0), Double(-1.0), "-")

	// Mul
	base = Int(2)
	verifyOp(t, base, Int(2), Int(4), "*")
	verifyOp(t, base, Double(2.0), Double(4.0), "*")
	base = Double(2.0)
	verifyOp(t, base, Int(2), Double(4.0), "*")
	verifyOp(t, base, Double(2.0), Double(4.0), "*")
	// Div
	base = Int(4)
	verifyOp(t, base, Int(2), Double(2.0), "/")
	verifyOp(t, base, Double(2.0), Double(2.0), "/")
	base = Double(4.0)
	verifyOp(t, base, Int(2), Double(2.0), "/")
	verifyOp(t, base, Double(2.0), Double(2.0), "/")

	// modulo
	base = Int(4)
	verifyOp(t, base, Int(2), Int(0), "%")
	verifyOp(t, base, Int(3), Int(1), "%")
	verifyOp(t, Int(-5), Int(3), Int(-2), "%")
	verifyOp(t, Int(5), Int(-3), Int(2), "%")
	verifyOp(t, Int(-5), Int(-3), Int(-2), "%")
}

func TestValidRelation(t *testing.T) {
	// Int
	var base Value
	base = Int(1)
	verifyOp(t, base, Int(1), Bool(false), ">")
	verifyOp(t, base, Int(1), Bool(true), ">=")
	verifyOp(t, base, Int(1), Bool(false), "<")
	verifyOp(t, base, Int(1), Bool(true), "<=")

	verifyOp(t, base, Double(1.0), Bool(false), ">")
	verifyOp(t, base, Double(1.0), Bool(true), ">=")
	verifyOp(t, base, Double(1.0), Bool(false), "<")
	verifyOp(t, base, Double(1.0), Bool(true), "<=")

	base = Double(1.0)
	verifyOp(t, base, Int(1), Bool(false), ">")
	verifyOp(t, base, Int(1), Bool(true), ">=")
	verifyOp(t, base, Int(1), Bool(false), "<")
	verifyOp(t, base, Int(1), Bool(true), "<=")

	verifyOp(t, base, Double(1.0), Bool(false), ">")
	verifyOp(t, base, Double(1.0), Bool(true), ">=")
	verifyOp(t, base, Double(1.0), Bool(false), "<")
	verifyOp(t, base, Double(1.0), Bool(true), "<=")
}

func TestBoolean(t *testing.T) {
	var base Value
	base = Bool(true)
	verifyOp(t, base, Bool(true), Bool(true), "and")
	verifyOp(t, base, Bool(false), Bool(false), "and")
	verifyOp(t, base, Bool(true), Bool(true), "or")
	verifyOp(t, base, Bool(false), Bool(true), "or")

	base = Bool(false)
	verifyOp(t, base, Bool(true), Bool(false), "and")
	verifyOp(t, base, Bool(false), Bool(false), "and")
	verifyOp(t, base, Bool(true), Bool(true), "or")
	verifyOp(t, base, Bool(false), Bool(false), "or")
}

func TestIndexList(t *testing.T) {
	l := List([]Value{Int(1), Double(2.0), Bool(true)})

	for i, expected := range l {
		found, err := l.Op("[]", Int(i))
		assert.NoError(t, err)
		assert.Equal(t, expected, found)
	}
	// but index error when using larger values or negative values
	_, err := l.Op("[]", Int(3))
	assert.Error(t, err)
	_, err = l.Op("[]", Int(-1))
	assert.Error(t, err)
}

func TestIndex_Dict(t *testing.T) {
	di := Dict(map[string]Value{"a": Int(2), "b": Double(1.0)})
	for k, expected := range di {
		found, err := di.Op("[]", String(k))
		assert.NoError(t, err)
		assert.Equal(t, expected, found)
	}
	// but index error when using strings that don't exist
	_, err := di.Op("[]", String("hello"))
	assert.Error(t, err)
}
