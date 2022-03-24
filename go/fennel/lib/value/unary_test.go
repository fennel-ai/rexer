package value

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func verifyUnaryOp(t *testing.T, op string, operand Value, expected Value) {
	ret, err := operand.OpUnary(op)
	assert.NoError(t, err)
	assert.Equal(t, expected, ret)

	// and verify future too
	f := getFuture(operand)
	fret, err := f.OpUnary(op)
	assert.NoError(t, err)
	assert.Equal(t, expected, fret)
}

func verifyUnaryError(t *testing.T, operand Value, ops []string) {
	for _, op := range ops {
		_, err := operand.OpUnary(op)
		assert.Error(t, err)
		// and also with future
		f := getFuture(operand)
		_, err = f.OpUnary(op)
		assert.Error(t, err)
	}
}

func TestUnaryInvalid(t *testing.T) {
	i := Int(-3)
	d := Double(-2.0)
	b := Bool(true)
	s := String("str")
	l := NewList(Int(-1), Double(-3.0), Bool(false))
	di := NewDict(map[string]Value{"z": Int(-3), "y": Double(-1.0)})
	n := Nil

	ops := []string{"!"}
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

	verifyUnaryError(t, i, ops)
	verifyUnaryError(t, d, ops)
	verifyUnaryError(t, b, allBut("!"))
	verifyUnaryError(t, s, ops)
	verifyUnaryError(t, l, ops)
	verifyUnaryError(t, di, ops)
	verifyUnaryError(t, n, ops)
}

func TestNot(t *testing.T) {
	verifyUnaryOp(t, "!", Bool(false), Bool(true))
	verifyUnaryOp(t, "!", Bool(true), Bool(false))
}
