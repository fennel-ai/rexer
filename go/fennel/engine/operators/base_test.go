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
	return NewSignature("test", "op").
		Param("p1", value.Types.Bool, true, false, value.Nil).
		Param("p2", value.Types.Double, false, false, value.Double(3.0)).
		Param("p3", value.Types.Any, true, false, value.Nil).
		Input([]value.Type{value.Types.String})
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
	return NewSignature("test", "op").
		Input([]value.Type{value.Types.String, value.Types.Int, value.Types.Any})
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
	return NewSignature("anothertest", "anotherop").Input(nil)
}

func TestTypeCheckStaticKwargs(t *testing.T) {
	t.Parallel()
	op := testOp{}
	scenarios := []struct {
		given   value.Dict
		matches bool
	}{
		{
			value.NewDict(map[string]value.Value{"p1": value.Bool(true), "p3": value.String("abc")}),
			true,
		},
		{
			value.NewDict(map[string]value.Value{"p1": value.Bool(false), "p2": value.Double(4.0)}),
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
	op1 := testOp{}  // expects exactly one input of string
	op2 := testOp2{} // expects 3 inputs: string, int, anything
	op3 := testOp3{} // expects any number of any inputs

	scenarios := []struct {
		op      Operator
		input   []value.Value
		context value.Dict
		matches bool
	}{
		{
			op1,
			[]value.Value{value.String("xyz")},
			value.NewDict(map[string]value.Value{"p2": value.Double(9.0)}),
			true,
		},
		{
			op1,
			[]value.Value{value.Int(2)},
			value.NewDict(map[string]value.Value{"p2": value.Double(9.0)}),
			false,
		},
		{
			op1,
			[]value.Value{value.Int(4)},
			value.NewDict(map[string]value.Value{"p2": value.Double(16.0)}),
			false,
		},
		{
			op1,
			[]value.Value{value.String("pqrs")},
			value.NewDict(map[string]value.Value{"p2": value.Int(3)}),
			false,
		},
		{
			op1,
			[]value.Value{value.String("ijk")},
			value.Dict{},
			false,
		},
		{
			op2,
			[]value.Value{value.String("ijk")},
			value.Dict{},
			false,
		},
		{
			op2,
			[]value.Value{},
			value.Dict{},
			false,
		},
		{
			op2,
			[]value.Value{value.String("jhi"), value.Int(4), value.Int(5)},
			value.Dict{},
			true,
		},
		{
			op2,
			[]value.Value{value.String("jhi"), value.Int(4), value.Nil},
			value.Dict{},
			true,
		},
		{
			op3,
			[]value.Value{value.String("jhi"), value.Int(4), value.Nil},
			value.Dict{},
			true,
		},
		{
			op3,
			[]value.Value{value.String("jhi")},
			value.Dict{},
			true,
		},
		{
			op3,
			[]value.Value{value.NewDict(map[string]value.Value{"jhi": value.Int(3)})},
			value.Dict{},
			true,
		},
	}
	for _, scenario := range scenarios {
		err := Typecheck(scenario.op, scenario.input, scenario.context)
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
