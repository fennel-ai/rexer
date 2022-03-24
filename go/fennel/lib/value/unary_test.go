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

	ops := []string{"~", "len"}
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

	verifyUnaryError(t, i, allBut("str"))
	verifyUnaryError(t, d, allBut("str"))
	verifyUnaryError(t, b, allBut("~", "str"))
	verifyUnaryError(t, s, allBut("str"))
	verifyUnaryError(t, l, allBut("len", "str"))
	verifyUnaryError(t, di, allBut("len", "str"))
	verifyUnaryError(t, n, allBut("str"))
}

func TestNot(t *testing.T) {
	verifyUnaryOp(t, "~", Bool(false), Bool(true))
	verifyUnaryOp(t, "~", Bool(true), Bool(false))
}

func TestLen(t *testing.T) {
	verifyUnaryOp(t, "len", NewList(), Int(0))
	verifyUnaryOp(t, "len", NewList(Nil, Bool(false), Int(0), Double(0.0), String("")), Int(5))
}

func TestStr(t *testing.T) {
	vals := []Value{
		Nil,
		Bool(false),
		Bool(true),
		Int(0),
		Int(12),
		Int(-13),
		Double(0),
		Double(7.5),
		Double(-6.5),
		String(""),
		String("pqrs"),
		NewList(),
		NewList(Nil, Bool(false), Int(0), Double(0), String("")),
		NewDict(map[string]Value{}),
		NewDict(map[string]Value{"": Nil, ".": Bool(true)}),
	}
	for _, v := range vals {
		verifyUnaryOp(t, "str", v, String(v.String()))
	}
}
