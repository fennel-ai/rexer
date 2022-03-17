package value

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func verifyOp(t *testing.T, left, right, expected Value, op string) {
	ret, err := left.Op(op, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, ret)

	// and verify future forms too
	lf := &Future{lock: sync.Mutex{}, fn: func() Value { return left }, cached: nil}
	rf := &Future{lock: sync.Mutex{}, fn: func() Value { return right }, cached: nil}
	fret, err := lf.Op(op, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, fret)

	fret, err = left.Op(op, rf)
	assert.NoError(t, err)
	assert.Equal(t, expected, fret)

	fret, err = lf.Op(op, rf)
	assert.NoError(t, err)
	assert.Equal(t, expected, fret)
}

func verifyError(t *testing.T, left, right Value, ops []string) {
	for _, op := range ops {
		_, err := left.Op(op, right)
		assert.Error(t, err)
		// and also with future
		lf := &Future{lock: sync.Mutex{}, fn: func() Value { return left }, cached: nil}
		rf := &Future{lock: sync.Mutex{}, fn: func() Value { return right }, cached: nil}
		_, err = lf.Op(op, right)
		assert.Error(t, err)

		_, err = left.Op(op, rf)
		assert.Error(t, err)

		_, err = lf.Op(op, rf)
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

	ops := []string{"+", "-", "*", "/", "//", ">", ">=", "<", "<=", "and", "or", "[]", "%"}
	allBut := func(xs ...string) []string {
		var res []string
		for _, op := range ops {
			valid := true
			for _, x := range xs {
				if op == x {
					valid = false
					break
				}
			}
			if valid {
				res = append(res, op)
			}
		}
		return res
	}

	// ints with themselves
	verifyError(t, i, i, []string{"and", "or", "[]"})
	// ints with others
	verifyError(t, i, d, []string{"and", "or", "[]", "%"})
	verifyError(t, i, b, ops)
	verifyError(t, i, s, ops)
	verifyError(t, i, l, ops)
	verifyError(t, i, di, ops)
	verifyError(t, i, n, ops)
	// and div/modulo throws an error when denominator is zero
	verifyError(t, i, Int(0), []string{"%", "/", "//"})
	verifyError(t, i, Double(0), []string{"%", "/", "//"})

	verifyError(t, d, i, []string{"and", "or", "%", "[]"})
	verifyError(t, d, d, []string{"and", "or", "%", "[]"})
	verifyError(t, d, b, ops)
	verifyError(t, d, s, ops)
	verifyError(t, d, l, ops)
	verifyError(t, d, di, ops)
	verifyError(t, d, n, ops)
	// and div throws an error when denominator is zero
	verifyError(t, d, Int(0), []string{"/", "//"})
	verifyError(t, d, Double(0), []string{"/", "//"})

	verifyError(t, b, i, ops)
	verifyError(t, b, d, ops)
	verifyError(t, b, b, allBut("and", "or"))
	verifyError(t, b, s, ops)
	verifyError(t, b, l, ops)
	verifyError(t, b, di, ops)
	verifyError(t, b, n, ops)

	verifyError(t, s, i, ops)
	verifyError(t, s, d, ops)
	verifyError(t, s, b, ops)
	// can only do concatenation with two strings
	verifyError(t, s, s, allBut("+"))
	verifyError(t, s, l, ops)
	verifyError(t, s, di, ops)
	verifyError(t, s, n, ops)

	// can only do indexing using a list and an int
	verifyError(t, l, i, allBut("[]"))
	verifyError(t, l, d, ops)
	verifyError(t, l, b, ops)
	verifyError(t, l, s, ops)
	// can only do concatenation with two lists
	verifyError(t, l, l, allBut("+"))
	verifyError(t, l, di, ops)
	verifyError(t, l, n, ops)

	verifyError(t, di, i, ops)
	verifyError(t, di, d, ops)
	verifyError(t, di, b, ops)
	// can only do an indexing on dictionary using a string
	verifyError(t, di, s, allBut("[]]"))
	verifyError(t, di, l, ops)
	verifyError(t, di, di, ops)
	verifyError(t, di, n, ops)

	verifyError(t, n, i, ops)
	verifyError(t, n, d, ops)
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
	verifyOp(t, base, Int(2), Double(2), "/")
	verifyOp(t, base, Int(8), Double(0.5), "/")
	verifyOp(t, base, Double(2.0), Double(2.0), "/")
	base = Double(4.0)
	verifyOp(t, base, Int(2), Double(2.0), "/")
	verifyOp(t, base, Double(2.0), Double(2.0), "/")

	// Floor Div
	base = Int(2)
	verifyOp(t, base, Int(1), Int(2), "//")
	verifyOp(t, base, Int(3), Int(0), "//")
	verifyOp(t, base, Double(3.0), Double(0.0), "//")
	verifyOp(t, base, Double(-3.0), Double(-1.0), "//")
	base = Double(2.0)
	verifyOp(t, base, Int(1), Double(2.0), "//")
	verifyOp(t, base, Double(3.0), Double(0.0), "//")
	verifyOp(t, base, Double(-3.0), Double(-1.0), "//")

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
	verifyOp(t, base, Int(1), Bool(true), "==")
	verifyOp(t, base, Int(1), Bool(false), "!=")

	verifyOp(t, base, Double(1.0), Bool(false), ">")
	verifyOp(t, base, Double(1.0), Bool(true), ">=")
	verifyOp(t, base, Double(1.0), Bool(false), "<")
	verifyOp(t, base, Double(1.0), Bool(true), "<=")
	verifyOp(t, base, Double(1.0), Bool(true), "==")
	verifyOp(t, base, Double(1.0), Bool(false), "!=")

	base = Double(1.0)
	verifyOp(t, base, Int(1), Bool(false), ">")
	verifyOp(t, base, Int(1), Bool(true), ">=")
	verifyOp(t, base, Int(1), Bool(false), "<")
	verifyOp(t, base, Int(1), Bool(true), "<=")
	verifyOp(t, base, Int(1), Bool(true), "==")
	verifyOp(t, base, Int(1), Bool(false), "!=")

	verifyOp(t, base, Double(1.0), Bool(false), ">")
	verifyOp(t, base, Double(1.0), Bool(true), ">=")
	verifyOp(t, base, Double(1.0), Bool(false), "<")
	verifyOp(t, base, Double(1.0), Bool(true), "<=")
	verifyOp(t, base, Double(1.0), Bool(true), "==")
	verifyOp(t, base, Double(1.0), Bool(false), "!=")
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

func testIndexList(t *testing.T, v Value, l List) {
	for i, expected := range l {
		found, err := v.Op("[]", Int(i))
		assert.NoError(t, err)
		assert.Equal(t, expected, found)
	}
	// but index error when using larger values or negative values
	_, err := v.Op("[]", Int(3))
	assert.Error(t, err)
	_, err = v.Op("[]", Int(-1))
	assert.Error(t, err)

}

func TestIndexList(t *testing.T) {
	l := List([]Value{Int(1), Double(2.0), Bool(true)})
	testIndexList(t, l, l)
	// also with futures
	testIndexList(t, &Future{
		lock: sync.Mutex{},
		fn: func() Value {
			return l
		},
		cached: nil,
	}, l)
}

func testIndex_Dict(t *testing.T, v Value, di Dict) {
	for k, expected := range di {
		found, err := v.Op("[]", String(k))
		assert.NoError(t, err)
		assert.Equal(t, expected, found)
	}
	// but index error when using strings that don't exist
	_, err := v.Op("[]", String("hello"))
	assert.Error(t, err)
}

func TestIndex_Dict(t *testing.T) {
	di := Dict(map[string]Value{"a": Int(2), "b": Double(1.0)})
	testIndex_Dict(t, di, di)
	// and also futures
	testIndex_Dict(t, &Future{
		lock: sync.Mutex{},
		fn: func() Value {
			return di
		},
		cached: nil,
	}, di)
}

func TestConcatenation(t *testing.T) {
	s1 := String("abc")
	s2 := String("xyz")
	verifyOp(t, s1, s2, String("abcxyz"), "+")

	l1 := List([]Value{Int(1), Nil})
	l2 := List([]Value{Double(2), Bool(false)})
	verifyOp(t, l1, l2, List([]Value{Int(1), Nil, Double(2), Bool(false)}), "+")
}
