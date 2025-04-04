package optest

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/engine/ast"
	"fennel/engine/interpreter"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	"fennel/lib/value"
	"fennel/tier"
)

func AssertEqual(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs [][]value.Value, context []value.Dict, expected []value.Value) {
	found, err := run(tr, op, static, inputs, context)
	assert.NoError(t, err)
	aslist, ok := found.(value.List)
	assert.True(t, ok)
	assert.Equal(t, len(expected), aslist.Len())

	for i, exp := range expected {
		e, err := aslist.At(i)
		assert.NoError(t, err)
		assert.True(t, exp.Equal(e), fmt.Sprintf("expected: %s, found: %s", exp, e))
	}
}

func AssertElementsMatch(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs [][]value.Value, context []value.Dict, expected []value.Value) {
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

func AssertError(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs [][]value.Value, context []value.Dict) {
	_, err := run(tr, op, static, inputs, context)
	assert.Error(t, err)
}

func AssertErrorIs(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs [][]value.Value, context []value.Dict, targetError error) {
	_, err := run(tr, op, static, inputs, context)
	assert.ErrorIs(t, err, targetError)
}

func AssertErrorContains(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs [][]value.Value, context []value.Dict, errorString string) {
	_, err := run(tr, op, static, inputs, context)
	assert.ErrorContains(t, err, errorString)
}

// run takes some value properties and creates a real ast that represents that opcall and executes it with
// an interpreter
func run(tr tier.Tier, op operators.Operator, static value.Dict, inputs [][]value.Value, queryContext []value.Dict) (value.Value, error) {
	sig := op.Signature()
	queryargs := value.NewDict(nil)
	kwargs := make(map[string]ast.Ast)

	// all static kwargs will be based on Var("static")
	for k := range static.Iter() {
		kwargs[k] = &ast.Lookup{
			On:       &ast.Var{Name: "static"},
			Property: k,
		}
	}
	queryargs.Set("static", static)
	// and input[i] is provided as Var("input_i"), except for the first input which is provided as Var("its")
	varnames := []string{"its"}
	for len(varnames) < len(inputs) {
		varnames = append(varnames, fmt.Sprintf("input_%d", len(varnames)))
	}
	asts := make([]ast.Ast, len(inputs))
	for i, input := range inputs {
		asts[i] = &ast.Var{Name: varnames[i]}
		queryargs.Set(varnames[i], value.NewList(input...))
	}
	field := "context"
	// context kwarg k will be accessible Var("field")[str(@)].k
	if len(queryContext) > 0 {
		for k := range queryContext[0].Iter() {
			kwargs[k] = &ast.Lookup{
				On: &ast.Binary{
					Left: &ast.Var{Name: field},
					Op:   "[]",
					Right: &ast.Unary{
						Op:      "str",
						Operand: &ast.Var{Name: "its"},
					},
				},
				Property: k,
			}
		}
	}
	kwargs_data := value.NewDict(nil)
	for i := range inputs[0] {
		k := inputs[0][i].String()
		kwargs_data.Set(k, queryContext[i])
	}
	queryargs.Set(field, kwargs_data)

	query := ast.OpCall{
		Operands:  asts,
		Vars:      varnames,
		Namespace: sig.Module,
		Name:      sig.Name,
		Kwargs:    ast.MakeDict(kwargs),
	}
	i, err := interpreter.NewInterpreter(context.Background(), bootarg.Create(tr), queryargs)
	if err != nil {
		return value.Nil, err
	}
	return query.AcceptValue(i)
}
