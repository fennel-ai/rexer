package operators

import (
	"fennel/lib/value"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testOp struct {
	num value.Int
}

var _ Operator = testOp{}

func (top testOp) Init(args value.Dict, bootargs map[string]interface{}) error {
	return nil
}

func (top testOp) Apply(kwargs value.Dict, in InputIter, out *value.Table) error {
	return nil
}

func (top testOp) Signature() *Signature {
	return NewSignature(top, "test", "op").
		Param("p1", value.Types.Bool, true, false, value.Nil).
		Param("p2", value.Types.Double, false, false, value.Double(3.0)).
		Param("p3", value.Types.Any, true, false, value.Nil).
		Input("c1", value.Types.String)
}

type testOp2 struct{}

var _ Operator = testOp2{}

func (top testOp2) Init(_ value.Dict, bootargs map[string]interface{}) error {
	return nil
}

func (top testOp2) Apply(_ value.Dict, _ InputIter, _ *value.Table) error {
	return nil
}

func (top testOp2) Signature() *Signature {
	return NewSignature(top, "test", "op")
}

type testOp3 struct{}

var _ Operator = testOp3{}

func (top testOp3) Init(_ value.Dict, bootargs map[string]interface{}) error {
	return nil
}

func (top testOp3) Apply(_ value.Dict, _ InputIter, _ *value.Table) error {
	return nil
}

func (top testOp3) Signature() *Signature {
	return NewSignature(top, "anothertest", "anotherop")
}

func TestTypecheck(t *testing.T) {
	op := testOp{}

	// if we don't pass all kwargsCorrect & inputIncorrect, it doesn't work
	kwargsCorrect := map[string]reflect.Type{"p1": value.Types.Bool, "p3": value.Types.String}
	kwargsIncorrect := map[string]reflect.Type{
		"p1": value.Types.Bool,
		"p2": value.Types.Double,
	}
	empty := map[string]reflect.Type{}
	inputIncorrect := map[string]reflect.Type{"c2": reflect.TypeOf(2)}
	inputCorrect := map[string]reflect.Type{"c1": value.Types.String}
	contextual := map[string]reflect.Type{"p2": value.Types.Double}

	assert.Error(t, Typecheck(op, kwargsCorrect, empty, empty))
	assert.Error(t, Typecheck(op, kwargsCorrect, inputIncorrect, empty))
	assert.Error(t, Typecheck(op, kwargsCorrect, inputIncorrect, empty))
	assert.Error(t, Typecheck(op, kwargsCorrect, inputIncorrect, contextual))
	assert.Error(t, Typecheck(op, kwargsCorrect, inputCorrect, empty))
	assert.Error(t, Typecheck(op, kwargsIncorrect, inputCorrect, contextual))

	// but it works when all are correct
	assert.NoError(t, Typecheck(op, kwargsCorrect, inputCorrect, contextual))

	// and for kwargs with type of any, really any type works
	kwargsCorrect = map[string]reflect.Type{"p1": value.Types.Bool, "p3": value.Types.List}
	assert.NoError(t, Typecheck(op, kwargsCorrect, inputCorrect, contextual))
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
