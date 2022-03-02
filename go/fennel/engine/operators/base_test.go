package operators

import (
	"reflect"
	"testing"

	"fennel/lib/value"

	"github.com/stretchr/testify/assert"
)

type testOp struct {
	num value.Int
}

var _ Operator = testOp{}

func (top testOp) Init(args value.Dict, bootargs map[string]interface{}) error {
	return nil
}

func (top testOp) Apply(kwargs value.Dict, in InputIter, out *value.List) error {
	return nil
}

func (top testOp) Signature() *Signature {
	return NewSignature("test", "op").
		Param("p1", value.Types.Bool, true, false, value.Nil).
		Param("p2", value.Types.Double, false, false, value.Double(3.0)).
		Param("p3", value.Types.Any, true, false, value.Nil).
		Input(value.Types.String)
}

type testOp2 struct{}

var _ Operator = testOp2{}

func (top testOp2) Init(_ value.Dict, bootargs map[string]interface{}) error {
	return nil
}

func (top testOp2) Apply(_ value.Dict, _ InputIter, _ *value.List) error {
	return nil
}

func (top testOp2) Signature() *Signature {
	return NewSignature("test", "op")
}

type testOp3 struct{}

var _ Operator = testOp3{}

func (top testOp3) Init(_ value.Dict, bootargs map[string]interface{}) error {
	return nil
}

func (top testOp3) Apply(_ value.Dict, _ InputIter, _ *value.List) error {
	return nil
}

func (top testOp3) Signature() *Signature {
	return NewSignature("anothertest", "anotherop")
}

func TestTypeCheckStaticKwargs(t *testing.T) {
	t.Parallel()
	op := testOp{}
	scenarios := []struct {
		given   map[string]reflect.Type
		matches bool
	}{
		{
			map[string]reflect.Type{"p1": value.Types.Bool, "p3": value.Types.String},
			true,
		},
		{
			map[string]reflect.Type{"p1": value.Types.Bool, "p2": value.Types.Double},
			false,
		},
		{
			map[string]reflect.Type{},
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
		input   reflect.Type
		context map[string]reflect.Type
		matches bool
	}{
		{
			value.Types.String,
			map[string]reflect.Type{"p2": value.Types.Double},
			true,
		},
		{
			reflect.TypeOf(2),
			map[string]reflect.Type{"p2": value.Types.Double},
			false,
		},
		{
			value.Types.Int,
			map[string]reflect.Type{"p2": value.Types.Double},
			false,
		},
		{
			value.Types.String,
			map[string]reflect.Type{"p2": value.Types.Int},
			false,
		},
		{
			value.Types.String,
			map[string]reflect.Type{},
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
