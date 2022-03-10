package operators

import (
	"testing"

	"fennel/lib/value"

	"github.com/stretchr/testify/assert"
)

type testOp struct {
	num value.Int
}

var _ Operator = testOp{}

func (top testOp) New(args value.Dict, bootargs map[string]interface{}) (Operator, error) {
	return top, nil
}

func (top testOp) Apply(kwargs value.Dict, in InputIter, out *value.List) error {
	return nil
}

func (top testOp) Signature() *Signature {
	return NewSignature("test", "op", false).
		Param("p1", value.Types.Bool, true, false, value.Nil).
		Param("p2", value.Types.Double, false, false, value.Double(3.0)).
		Param("p3", value.Types.Any, true, false, value.Nil).
		Input(value.Types.String)
}

type testOp2 struct{}

var _ Operator = testOp2{}

func (top testOp2) New(args value.Dict, bootargs map[string]interface{}) (Operator, error) {
	return top, nil
}

func (top testOp2) Apply(_ value.Dict, _ InputIter, _ *value.List) error {
	return nil
}

func (top testOp2) Signature() *Signature {
	return NewSignature("test", "op", true)
}

type testOp3 struct{}

var _ Operator = testOp3{}

func (top testOp3) New(args value.Dict, bootargs map[string]interface{}) (Operator, error) {
	return top, nil
}

func (top testOp3) Apply(_ value.Dict, _ InputIter, _ *value.List) error {
	return nil
}

func (top testOp3) Signature() *Signature {
	return NewSignature("anothertest", "anotherop", true)
}

func TestTypeCheckStaticKwargs(t *testing.T) {
	t.Parallel()
	op := testOp{}
	scenarios := []struct {
		given   value.Dict
		matches bool
	}{
		{
			value.Dict{"p1": value.Bool(true), "p3": value.String("abc")},
			true,
		},
		{
			value.Dict{"p1": value.Bool(false), "p2": value.Double(4.0)},
			false,
		},
		{
			value.Dict{},
			false,
		},
	}
	for _, scenario := range scenarios {
		err := TypeCheckStaticKwargs(op, scenario.given)
		if scenario.matches {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func TestTypeCheck(t *testing.T) {
	t.Parallel()
	op := testOp{}

	scenarios := []struct {
		input   value.Value
		context value.Dict
		matches bool
	}{
		{
			value.String("xyz"),
			value.Dict{"p2": value.Double(9.0)},
			true,
		},
		{
			value.Int(2),
			value.Dict{"p2": value.Double(9.0)},
			false,
		},
		{
			value.Int(4),
			value.Dict{"p2": value.Double(16.0)},
			false,
		},
		{
			value.String("pqrs"),
			value.Dict{"p2": value.Int(3)},
			false,
		},
		{
			value.String("ijk"),
			value.Dict{},
			false,
		},
	}
	for _, scenario := range scenarios {
		err := Typecheck(op, scenario.input, scenario.context)
		if scenario.matches {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func TestRegister(t *testing.T) {
	err := Register(testOp{})
	assert.NoError(t, err)

	// trying to register same Name/module again doesn't work
	err = Register(testOp2{})
	assert.Error(t, err)

	// but if we change either of Name/module, it will work
	err = Register(testOp3{})
	assert.NoError(t, err)
}

func TestGetOperatorsJSON(t *testing.T) {
	_, err := GetOperatorsJSON()
	assert.NoError(t, err)
}

func TestIsMapper(t *testing.T) {
	assert.False(t, IsMapper(testOp{}))
	assert.True(t, IsMapper(testOp2{}))
	assert.True(t, IsMapper(testOp3{}))
}
