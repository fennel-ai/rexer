package value

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func verifyBinaryOp(t *testing.T, left, right, expected Value, op string) {
	ret, err := left.Op(op, right)
	assert.NoError(t, err)
	assert.Equal(t, expected, ret)

	// and verify future forms too
	lf := getFuture(left)
	rf := getFuture(right)
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

func verifyBinaryError(t *testing.T, left, right Value, ops []string) {
	for _, op := range ops {
		_, err := left.Op(op, right)
		assert.Error(t, err)
		// and also with future
		lf := getFuture(left)
		rf := getFuture(right)
		_, err = lf.Op(op, right)
		assert.Error(t, err)

		_, err = left.Op(op, rf)
		assert.Error(t, err)

		_, err = lf.Op(op, rf)
		assert.Error(t, err)
	}
}

func TestBinaryInvalid(t *testing.T) {
	i := Int(2)
	d := Double(3.0)
	b := Bool(false)
	s := String("hi")
	l := NewList(Int(1), Double(2.0), Bool(true))
	di := NewDict(map[string]Value{"a": Int(2), "b": Double(1.0)})
	n := Nil

	ops := []string{"+", "-", "*", "/", "//", ">", ">=", "<", "<=", "and", "or", "[]", "%", "in"}
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
	verifyBinaryError(t, i, i, []string{"and", "or", "[]"})
	// ints with others
	verifyBinaryError(t, i, d, []string{"and", "or", "[]", "%"})
	verifyBinaryError(t, i, b, ops)
	verifyBinaryError(t, i, s, ops)
	verifyBinaryError(t, i, l, allBut("in"))
	verifyBinaryError(t, i, di, ops)
	verifyBinaryError(t, i, n, ops)
	// and div/modulo throws an error when denominator is zero
	verifyBinaryError(t, i, Int(0), []string{"%", "/", "//"})
	verifyBinaryError(t, i, Double(0), []string{"%", "/", "//"})

	verifyBinaryError(t, d, i, []string{"and", "or", "%", "[]"})
	verifyBinaryError(t, d, d, []string{"and", "or", "%", "[]"})
	verifyBinaryError(t, d, b, ops)
	verifyBinaryError(t, d, s, ops)
	verifyBinaryError(t, d, l, allBut("in"))
	verifyBinaryError(t, d, di, ops)
	verifyBinaryError(t, d, n, ops)
	// and div throws an error when denominator is zero
	verifyBinaryError(t, d, Int(0), []string{"/", "//"})
	verifyBinaryError(t, d, Double(0), []string{"/", "//"})

	verifyBinaryError(t, b, i, ops)
	verifyBinaryError(t, b, d, ops)
	verifyBinaryError(t, b, b, allBut("and", "or"))
	verifyBinaryError(t, b, s, ops)
	verifyBinaryError(t, b, l, allBut("in"))
	verifyBinaryError(t, b, di, ops)
	verifyBinaryError(t, b, n, ops)

	verifyBinaryError(t, s, i, ops)
	verifyBinaryError(t, s, d, ops)
	verifyBinaryError(t, s, b, ops)
	// can only do concatenation with two strings
	verifyBinaryError(t, s, s, allBut("+"))
	verifyBinaryError(t, s, l, allBut("in"))
	verifyBinaryError(t, s, di, allBut("in"))
	verifyBinaryError(t, s, n, ops)

	// can only do indexing using a list and an int
	verifyBinaryError(t, l, i, allBut("[]"))
	verifyBinaryError(t, l, d, ops)
	verifyBinaryError(t, l, b, ops)
	verifyBinaryError(t, l, s, ops)
	// can only do concatenation with two lists
	verifyBinaryError(t, l, l, allBut("+", "in"))
	verifyBinaryError(t, l, di, ops)
	verifyBinaryError(t, l, n, ops)

	verifyBinaryError(t, di, i, ops)
	verifyBinaryError(t, di, d, ops)
	verifyBinaryError(t, di, b, ops)
	// can only do an indexing on dictionary using a string
	verifyBinaryError(t, di, s, allBut("[]]"))
	verifyBinaryError(t, di, l, allBut("in"))
	verifyBinaryError(t, di, di, ops)
	verifyBinaryError(t, di, n, ops)

	verifyBinaryError(t, n, i, ops)
	verifyBinaryError(t, n, d, ops)
	verifyBinaryError(t, n, b, ops)
	verifyBinaryError(t, n, s, ops)
	verifyBinaryError(t, n, l, allBut("in"))
	verifyBinaryError(t, n, di, ops)
	verifyBinaryError(t, n, n, ops)
}

func TestValidArithmetic(t *testing.T) {
	// Add
	var base Value
	base = Int(1)
	verifyBinaryOp(t, Int(1), Int(2), Int(3), "+")
	verifyBinaryOp(t, Int(1), Double(2.0), Double(3.0), "+")
	base = Double(1.0)
	verifyBinaryOp(t, base, Int(2), Double(3.0), "+")
	verifyBinaryOp(t, base, Double(2.0), Double(3.0), "+")

	// Sub
	base = Int(1)
	verifyBinaryOp(t, Int(1), Int(2), Int(-1), "-")
	verifyBinaryOp(t, Int(1), Double(2.0), Double(-1.0), "-")
	base = Double(1.0)
	verifyBinaryOp(t, base, Int(2), Double(-1.0), "-")
	verifyBinaryOp(t, base, Double(2.0), Double(-1.0), "-")

	// Mul
	base = Int(2)
	verifyBinaryOp(t, base, Int(2), Int(4), "*")
	verifyBinaryOp(t, base, Double(2.0), Double(4.0), "*")
	base = Double(2.0)
	verifyBinaryOp(t, base, Int(2), Double(4.0), "*")
	verifyBinaryOp(t, base, Double(2.0), Double(4.0), "*")

	// Div
	base = Int(4)
	verifyBinaryOp(t, base, Int(2), Double(2), "/")
	verifyBinaryOp(t, base, Int(8), Double(0.5), "/")
	verifyBinaryOp(t, base, Double(2.0), Double(2.0), "/")
	base = Double(4.0)
	verifyBinaryOp(t, base, Int(2), Double(2.0), "/")
	verifyBinaryOp(t, base, Double(2.0), Double(2.0), "/")

	// Floor Div
	base = Int(2)
	verifyBinaryOp(t, base, Int(1), Int(2), "//")
	verifyBinaryOp(t, base, Int(3), Int(0), "//")
	verifyBinaryOp(t, base, Double(3.0), Double(0.0), "//")
	verifyBinaryOp(t, base, Double(-3.0), Double(-1.0), "//")
	base = Double(2.0)
	verifyBinaryOp(t, base, Int(1), Double(2.0), "//")
	verifyBinaryOp(t, base, Double(3.0), Double(0.0), "//")
	verifyBinaryOp(t, base, Double(-3.0), Double(-1.0), "//")

	// modulo
	base = Int(4)
	verifyBinaryOp(t, base, Int(2), Int(0), "%")
	verifyBinaryOp(t, base, Int(3), Int(1), "%")
	verifyBinaryOp(t, Int(-5), Int(3), Int(-2), "%")
	verifyBinaryOp(t, Int(5), Int(-3), Int(2), "%")
	verifyBinaryOp(t, Int(-5), Int(-3), Int(-2), "%")
}

func TestValidRelation(t *testing.T) {
	// Int
	var base Value
	base = Int(1)
	verifyBinaryOp(t, base, Int(1), Bool(false), ">")
	verifyBinaryOp(t, base, Int(1), Bool(true), ">=")
	verifyBinaryOp(t, base, Int(1), Bool(false), "<")
	verifyBinaryOp(t, base, Int(1), Bool(true), "<=")
	verifyBinaryOp(t, base, Int(1), Bool(true), "==")
	verifyBinaryOp(t, base, Int(1), Bool(false), "!=")

	verifyBinaryOp(t, base, Double(1.0), Bool(false), ">")
	verifyBinaryOp(t, base, Double(1.0), Bool(true), ">=")
	verifyBinaryOp(t, base, Double(1.0), Bool(false), "<")
	verifyBinaryOp(t, base, Double(1.0), Bool(true), "<=")
	verifyBinaryOp(t, base, Double(1.0), Bool(true), "==")
	verifyBinaryOp(t, base, Double(1.0), Bool(false), "!=")

	base = Double(1.0)
	verifyBinaryOp(t, base, Int(1), Bool(false), ">")
	verifyBinaryOp(t, base, Int(1), Bool(true), ">=")
	verifyBinaryOp(t, base, Int(1), Bool(false), "<")
	verifyBinaryOp(t, base, Int(1), Bool(true), "<=")
	verifyBinaryOp(t, base, Int(1), Bool(true), "==")
	verifyBinaryOp(t, base, Int(1), Bool(false), "!=")

	verifyBinaryOp(t, base, Double(1.0), Bool(false), ">")
	verifyBinaryOp(t, base, Double(1.0), Bool(true), ">=")
	verifyBinaryOp(t, base, Double(1.0), Bool(false), "<")
	verifyBinaryOp(t, base, Double(1.0), Bool(true), "<=")
	verifyBinaryOp(t, base, Double(1.0), Bool(true), "==")
	verifyBinaryOp(t, base, Double(1.0), Bool(false), "!=")
}

func TestBoolean(t *testing.T) {
	var base Value
	base = Bool(true)
	verifyBinaryOp(t, base, Bool(true), Bool(true), "and")
	verifyBinaryOp(t, base, Bool(false), Bool(false), "and")
	verifyBinaryOp(t, base, Bool(true), Bool(true), "or")
	verifyBinaryOp(t, base, Bool(false), Bool(true), "or")

	base = Bool(false)
	verifyBinaryOp(t, base, Bool(true), Bool(false), "and")
	verifyBinaryOp(t, base, Bool(false), Bool(false), "and")
	verifyBinaryOp(t, base, Bool(true), Bool(true), "or")
	verifyBinaryOp(t, base, Bool(false), Bool(false), "or")
}

func testIndexList(t *testing.T, v Value, l List) {
	for i, expected := range l.values {
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
	l := NewList(Int(1), Double(2.0), Bool(true))
	testIndexList(t, l, l)
	// also with futures
	testIndexList(t, getFuture(l), l)
}

func testIndexDict(t *testing.T, v Value, di *Dict) {
	for k, expected := range di.Iter() {
		found, err := v.Op("[]", String(k))
		assert.NoError(t, err)
		assert.Equal(t, expected, found)
	}
	// but index error when using strings that don't exist
	_, err := v.Op("[]", String("hello"))
	assert.Error(t, err)
}

func TestIndexDict(t *testing.T) {
	di := NewDict(map[string]Value{"a": Int(2), "b": Double(1.0)})
	testIndexDict(t, di, di)
	// and also futures
	testIndexDict(t, getFuture(di), di)
}

func TestConcatenation(t *testing.T) {
	s1 := String("abc")
	s2 := String("xyz")
	verifyBinaryOp(t, s1, s2, String("abcxyz"), "+")

	l1 := NewList(Int(1), Nil)
	l2 := NewList(Double(2), Bool(false))
	verifyBinaryOp(t, l1, l2, NewList(Int(1), Nil, Double(2), Bool(false)), "+")
}

func getFuture(v Value) *Future {
	ch := make(chan Value, 1)
	ch <- v
	return &Future{
		lock:   sync.Mutex{},
		ch:     ch,
		cached: nil,
	}
}

func TestContains_Valid(t *testing.T) {
	scenarios := []struct {
		left  Value
		right Value
		exp   Value
	}{
		{
			String("hi"),
			NewList(Int(1), Int(2), String("hi")),
			Bool(true),
		},
		{
			String("bye"),
			NewList(Int(1), Int(2), String("hi")),
			Bool(false),
		},
		{
			NewDict(map[string]Value{"x": Int(1), "y": Int(2)}),
			NewList(Int(1), Int(2), String("hi"), NewDict(map[string]Value{"x": Int(1), "y": Int(2)})),
			Bool(true),
		},
		{
			String("x"),
			NewDict(map[string]Value{"x": Int(1), "y": Int(2)}),
			Bool(true),
		},
		{
			String("bye"),
			NewDict(map[string]Value{"x": Int(1), "y": Int(2)}),
			Bool(false),
		},
	}
	for i, scene := range scenarios {
		found, err := scene.left.Op("in", scene.right)
		msg := fmt.Sprintf("[%d] %s in %s = %s", i, scene.left, scene.right, scene.exp)
		assert.NoError(t, err, msg)
		assert.Equal(t, scene.exp, found, msg)
	}
}
