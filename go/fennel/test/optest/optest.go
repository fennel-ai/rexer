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

func AssertEqual(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs []value.Value, context []value.Dict, expected []value.Value) {
	found, err := run(tr, op, static, inputs, context)
	assert.NoError(t, err)
	aslist, ok := found.(value.List)
	assert.True(t, ok)
	assert.Equal(t, len(expected), aslist.Len())
	for i, exp := range expected {
		e, err := aslist.At(i)
		assert.NoError(t, err)
		assert.True(t, exp.Equal(e))
	}
}

func AssertElementsMatch(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs []value.Value, context []value.Dict, expected []value.Value) {
	found, err := run(tr, op, static, inputs, context)
	assert.NoError(t, err)
	aslist, ok := found.(value.List)
	assert.True(t, ok)
	assert.Equal(t, len(expected), aslist.Len())
	foundlist := make([]value.Value, aslist.Len())
	for i := 0; i < aslist.Len(); i++ {
		foundlist[i], _ = aslist.At(i)
	}
	assert.ElementsMatch(t, expected, foundlist)
}

func AssertError(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs []value.Value, context []value.Dict) {
	_, err := run(tr, op, static, inputs, context)
	assert.Error(t, err)
}

// run takes some value properties and creates a real ast that represents that opcall and executes it with
// an interpreter
func run(tr tier.Tier, op operators.Operator, static value.Dict, inputs []value.Value, context []value.Dict) (value.Value, error) {
	sig := op.Signature()
	kwargs := make(map[string]ast.Ast)

	// all static kwargs will be based on Var("args").static
	for k, _ := range static.Iter() {
		kwargs[k] = ast.Lookup{
			On:       ast.Lookup{On: ast.Var{Name: "args"}, Property: "static"},
			Property: k,
		}
	}
	field := "context"
	// context kwarg k will be accessible via Var("args").field[str(@)].k
	if len(context) > 0 {
		for k, _ := range context[0].Iter() {
			kwargs[k] = ast.Lookup{
				On: ast.Binary{
					Left: ast.Lookup{On: ast.Var{Name: "args"}, Property: field},
					Op:   "[]",
					Right: ast.Unary{
						Op:      "str",
						Operand: ast.Var{Name: "its"},
					},
				},
				Property: k,
			}
		}
	}
	kwargs_data := value.NewDict(nil)
	for i := range inputs {
		k := inputs[i].String()
		kwargs_data.Set(k, context[i])
	}

	// and input is provided as Var("args").input
	query := ast.OpCall{
		Operands: []ast.Ast{ast.Lookup{
			On:       ast.Var{Name: "args"},
			Property: "input",
		}},
		Vars:      []string{"its"},
		Namespace: sig.Module,
		Name:      sig.Name,
		Kwargs:    ast.Dict{Values: kwargs},
	}
	l := value.List{}
	for i := range inputs {
		l.Append(inputs[i])
	}
	i := interpreter.NewInterpreter(bootarg.Create(tr))
	return i.Eval(query, value.NewDict(map[string]value.Value{"input": l, "static": static, field: kwargs_data}))
}
