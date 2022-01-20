package operators

import (
	"fennel/lib/value"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testOp struct{}

var _ Operator = testOp{}

func (top testOp) Apply(kwargs value.Dict, in value.Table, out *value.Table) error {
	return nil
}

func (top testOp) Signature() *Signature {
	return NewSignature().
		Param("p1", reflect.TypeOf(value.Bool(true))).
		Param("p2", reflect.TypeOf(value.Double(1.0))).
		Input("c1", reflect.TypeOf(value.String("hi")))
}

func TestSignature(t *testing.T) {
	op := testOp{}

	// if we don't pass all kwargs & inputs, it doesn't work
	kwargs := value.Dict(map[string]value.Value{
		"p1": value.Bool(false),
	})
	inputs := map[string]reflect.Type{}
	assert.Error(t, Validate(op, kwargs, inputs))
	inputs = map[string]reflect.Type{"c2": reflect.TypeOf(2)}
	assert.Error(t, Validate(op, kwargs, inputs))

	inputs = map[string]reflect.Type{"c1": reflect.TypeOf(value.String("a"))}
	assert.Error(t, Validate(op, kwargs, inputs))

	kwargs = value.Dict(map[string]value.Value{
		"p1": value.Bool(false),
		"p2": value.Double(1.0),
	})
	inputs = map[string]reflect.Type{}
	assert.Error(t, Validate(op, kwargs, inputs))

	// but it works when all are correct
	kwargs = value.Dict(map[string]value.Value{
		"p1": value.Bool(false),
		"p2": value.Double(1.0),
	})
	inputs = map[string]reflect.Type{
		"c1": reflect.TypeOf(value.String("hi")),
	}
	assert.NoError(t, Validate(op, kwargs, inputs))
}
