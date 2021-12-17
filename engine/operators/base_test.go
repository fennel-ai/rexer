package operators

import (
	"engine/runtime"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

type testOp struct{}

var _ Operator = testOp{}

func (top testOp) Apply(kwargs runtime.Dict, in runtime.Table, out *runtime.Table) error {
	return nil
}

func (top testOp) Signature() *Signature {
	return NewSignature().
		Param("p1", reflect.TypeOf(runtime.Bool(true))).
		Param("p2", reflect.TypeOf(runtime.Double(1.0))).
		Input("c1", reflect.TypeOf(runtime.String("hi")))
}

func TestSignature(t *testing.T) {
	op := testOp{}

	// if we don't pass all kwargs & inputs, it doesn't work
	kwargs := runtime.Dict(map[string]runtime.Value{
		"p1": runtime.Bool(false),
	})
	inputs := map[string]reflect.Type{}
	assert.Error(t, Validate(op, kwargs, inputs))
	inputs = map[string]reflect.Type{"c2": reflect.TypeOf(2)}
	assert.Error(t, Validate(op, kwargs, inputs))

	inputs = map[string]reflect.Type{"c1": reflect.TypeOf(runtime.String("a"))}
	assert.Error(t, Validate(op, kwargs, inputs))

	kwargs = runtime.Dict(map[string]runtime.Value{
		"p1": runtime.Bool(false),
		"p2": runtime.Double(1.0),
	})
	inputs = map[string]reflect.Type{}
	assert.Error(t, Validate(op, kwargs, inputs))

	// but it works when all are correct
	kwargs = runtime.Dict(map[string]runtime.Value{
		"p1": runtime.Bool(false),
		"p2": runtime.Double(1.0),
	})
	inputs = map[string]reflect.Type{
		"c1": reflect.TypeOf(runtime.String("hi")),
	}
	assert.NoError(t, Validate(op, kwargs, inputs))
}
