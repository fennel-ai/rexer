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

func Assert(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs, context []value.Dict, expected []value.Value) {
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

func AssertEqual(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs, context []value.Dict, expected []value.Value) {
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

func AssertElementsMatch(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs, context []value.Dict, expected []value.Value) {
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

func AssertError(t *testing.T, tr tier.Tier, op operators.Operator, static value.Dict, inputs, context []value.Dict) {
	_, err := run(tr, op, static, inputs, context)
	assert.Error(t, err)
}

// run takes some value properties and creates a real ast that represents that opcall and executes it with
// an interpreter
func run(tr tier.Tier, op operators.Operator, static value.Dict, inputs, context []value.Dict) (value.Value, error) {
	sig := op.Signature()
	kwargs := make(map[string]ast.Ast)
	// all static kwargs will be based on Var("static")
	for k, _ := range static.Iter() {
		kwargs[k] = ast.Lookup{
			On:       ast.Var{Name: "static"},
			Property: k,
		}
	}
	field := "__test__context__"
	// context kwarg k will be accessible via Var("its").field.k
	if len(context) > 0 {
		for k, _ := range context[0].Iter() {
			kwargs[k] = ast.Lookup{
				On: ast.Lookup{
					On:       ast.Var{Name: "its"},
					Property: field,
				},
				Property: k,
			}
		}
	}
	// actually augment input to have the field so that context kwargs can be read
	for i := range inputs {
		inputs[i].Set(field, context[i])
	}

	// and input is provided as Var("input")
	query := ast.OpCall{
		Operands: []ast.Ast{ast.Var{
			Name: "input",
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
	found, err := i.Eval(query, value.NewDict(map[string]value.Value{"input": l, "static": static}))
	// before returning, remove the extra field that we had added
	if err == nil {
		found = cleanup(found, field)
	}
	return found, err
}

func cleanup(v value.Value, f string) value.Value {
	switch t := v.(type) {
	case value.List:
		ret := value.NewList()
		for i := 0; i < t.Len(); i++ {
			e, _ := t.At(i)
			ret.Append(cleanup(e, f))
		}
		return ret
	case value.Dict:
		ret := value.NewDict(nil)
		for k, v := range t.Iter() {
			if k == f {
				continue
			}
			ret.Set(k, cleanup(v, f))
		}
		return ret
	default:
		return t
	}
}
