package optest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/engine/ast"
	"fennel/engine/interpreter"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	"fennel/lib/value"
	"fennel/tier"
)

func Assert(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs, context []value.Dict, expected []value.Dict) {
	found, err := run(tr, op, static, inputs, context)
	assert.NoError(t, err)
	aslist, ok := found.(value.List)
	assert.True(t, ok)
	assert.Len(t, aslist, len(expected))
	assert.ElementsMatch(t, expected, found)
}

func AssertError(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs, context []value.Dict) {
	_, err := run(tr, op, static, inputs, context)
	assert.Error(t, err)
}

// run takes some value properties and creates a real ast that represents that opcall and executes it with
// an interpreter
func run(tr tier.Tier, op operators.Operator, static value.Dict, inputs, context []value.Dict) (value.Value, error) {
	sig := op.Signature()
	kwargs := make(map[string]ast.Ast)
	// all static kwargs will be based on Var("args").static
	for k, _ := range static {
		kwargs[k] = ast.Lookup{
			On:       ast.Lookup{ast.Var{"args"}, "static"},
			Property: k,
		}
	}
	field := "__test__context__"
	// context kwarg k will be accessible via it.field.k
	if len(context) > 0 {
		for k, _ := range context[0] {
			kwargs[k] = ast.Lookup{
				On: ast.Lookup{
					On:       ast.At{},
					Property: field,
				},
				Property: k,
			}
		}
	}
	// actually augment input to have the field so that context kwargs can be read
	for i := range inputs {
		inputs[i][field] = context[i]
	}

	// and input is provided as Var("args").input
	query := ast.OpCall{
		Operand: ast.Lookup{
			On:       ast.Var{"args"},
			Property: "input",
		},
		Namespace: sig.Module,
		Name:      sig.Name,
		Kwargs:    ast.Dict{Values: kwargs},
	}
	l := value.List{}
	for i := range inputs {
		l.Append(inputs[i])
	}
	i := interpreter.NewInterpreter(bootarg.Create(tr))
	found, err := i.Eval(query, value.Dict{"input": l, "static": static})
	aslist, ok := found.(value.List)
	// before returning, remove the extra field that we had added
	if err == nil && ok {
		for i := range aslist {
			delete(aslist[i].(value.Dict), field)
		}
	}
	return found, err
}
