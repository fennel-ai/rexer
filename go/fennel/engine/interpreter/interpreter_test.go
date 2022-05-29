package interpreter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"fennel/engine/ast"
	"fennel/lib/value"
	_ "fennel/opdefs/std"
	_ "fennel/opdefs/std/map"
	_ "fennel/opdefs/std/set"
)

func getInterpreter(bootargs map[string]interface{}, args value.Dict) Interpreter {
	if bootargs == nil {
		bootargs = map[string]interface{}{}
	}
	i, _ := NewInterpreter(context.Background(), bootargs, args)
	return i
}

func testValid(t *testing.T, node ast.Ast, expected value.Value) {
	i := getInterpreter(nil, value.Dict{})
	ret, err := node.AcceptValue(i)
	assert.NoError(t, err)
	assert.Equal(t, expected, ret)
}

func testError(t *testing.T, node ast.Ast) {
	i := getInterpreter(nil, value.Dict{})
	_, err := node.AcceptValue(i)
	assert.Error(t, err)
}

func TestInterpreter_VisitAtom(t *testing.T) {
	testValid(t, ast.MakeInt(123), value.Int(123))
	testValid(t, ast.MakeInt(-123), value.Int(-123))
	// possible to parse double without decimal
	testValid(t, ast.MakeDouble(123), value.Double(123.0))
	testValid(t, ast.MakeDouble(123.3), value.Double(123.3))
	testValid(t, ast.MakeDouble(-123.3), value.Double(-123.3))

	testValid(t, ast.MakeBool(true), value.Bool(true))
	testValid(t, ast.MakeBool(false), value.Bool(false))

	testValid(t, ast.MakeString("hi"), value.String("hi"))
	testValid(t, ast.MakeString("false"), value.String("false"))
	testValid(t, ast.MakeString("3.2"), value.String("3.2"))
}

func TestInterpreter_VisitUnary(t *testing.T) {
	testValid(t, ast.Unary{
		Op:      "~",
		Operand: ast.MakeBool(false),
	}, value.Bool(true))

	testError(t, ast.Unary{
		Op:      "~",
		Operand: ast.MakeInt(4),
	})
}

func TestInterpreter_VisitBinary(t *testing.T) {
	testValid(t, ast.Binary{
		Left:  ast.MakeInt(5),
		Op:    "+",
		Right: ast.MakeInt(8),
	}, value.Int(13))

	// and errors are propagated through type errors.
	testError(t, ast.Binary{
		Left:  ast.MakeInt(5),
		Op:    "*",
		Right: ast.MakeBool(false),
	})
}

func TestInterpreter_VisitBinary_Shortcircuit_Bool(t *testing.T) {
	t.Parallel()
	invalid := ast.Binary{Left: ast.MakeInt(1), Op: "*", Right: ast.MakeString("hi")}
	// verify this node throws error when evaluated
	testError(t, invalid)

	// but no error when short circuit happens
	testValid(t, ast.Binary{Left: ast.MakeBool(true), Op: "or", Right: invalid}, value.Bool(true))
	testValid(t, ast.Binary{Left: ast.MakeBool(false), Op: "and", Right: invalid}, value.Bool(false))
	// and same happens when valuates evaluate to bool but aren't bool asts to start with
	testValid(t, ast.Binary{
		Left:  ast.Binary{Left: ast.MakeInt(1), Op: ">=", Right: ast.MakeDouble(3.1)},
		Op:    "and",
		Right: invalid,
	}, value.Bool(false))
	testValid(t, ast.Binary{Left: ast.Binary{Left: ast.MakeInt(6), Op: ">=", Right: ast.MakeDouble(3.1)},
		Op:    "or",
		Right: invalid,
	}, value.Bool(true))

	// and error comes again when short circuit doesn't happen
	testError(t, ast.Binary{Left: ast.MakeBool(false), Op: "or", Right: invalid})
	testError(t, ast.Binary{Left: ast.MakeBool(true), Op: "and", Right: invalid})
}

func TestInterpreter_VisitList(t *testing.T) {
	// Empty list works
	testValid(t, ast.List{Values: []ast.Ast{}}, value.NewList())
	// list with just one element works
	l := value.NewList(value.Double(3.4))
	testValid(t, &ast.List{Values: []ast.Ast{ast.MakeDouble(3.4)}}, l)
	// and so does a multi-element list with mixed types
	l = value.NewList(value.Double(3.4), value.Bool(false), value.String("hi"))
	testValid(t, &ast.List{Values: []ast.Ast{ast.MakeDouble(3.4), ast.MakeBool(false), ast.MakeString("hi")}}, l)
}

func TestInterpreter_VisitDict(t *testing.T) {
	// Empty dict works
	testValid(t, ast.Dict{Values: map[string]ast.Ast{}}, value.NewDict(nil))

	// dict with just one element works
	d := value.NewDict(map[string]value.Value{"hi": value.Double(3.4)})
	testValid(t, ast.Dict{Values: map[string]ast.Ast{"hi": ast.MakeDouble(3.4)}}, d)
	// and so does a multi-element list with mixed types and nesting
	nested := value.NewDict(map[string]value.Value{
		"hi":     value.Double(3.4),
		"bye":    value.Bool(false),
		"nested": d,
	})
	testValid(t, ast.Dict{Values: map[string]ast.Ast{
		"hi":  ast.MakeDouble(3.4),
		"bye": ast.MakeBool(false),
		"nested": ast.Dict{
			Values: map[string]ast.Ast{
				"hi": ast.MakeDouble(3.4),
			}}}}, nested)
}

func TestInterpreter_VisitStatement(t *testing.T) {
	s := ast.Statement{Name: "var", Body: ast.MakeBool(false)}
	testValid(t, s, value.Bool(false))

	// same happens if no name is passed
	s = ast.Statement{Name: "", Body: ast.MakeBool(false)}
	testValid(t, s, value.Bool(false))
	// but if name is passed and it is a magic name, error is thrown
	s = ast.Statement{Name: "__something__", Body: ast.MakeBool(false)}
	testError(t, s)
}

func TestInterpreter_QueryArgs(t *testing.T) {
	i := getInterpreter(nil, value.Dict{})
	// initially nothing
	assert.True(t, value.NewDict(nil).Equal(i.queryArgs()))
	// queryargs are found at the root env
	args := value.NewDict(map[string]value.Value{"x": value.Int(0)})
	i = getInterpreter(nil, args)
	assert.NoError(t, i.env.Define("__args__", args))
	assert.True(t, args.Equal(i.queryArgs()))
	// should work even if we create child envs
	i.env = i.env.PushEnv()
	i.env = i.env.PushEnv()
	assert.True(t, args.Equal(i.queryArgs()))
	// or even if we shadow query args
	assert.NoError(t, i.env.Define("__args__", value.String("ijk")))
	assert.True(t, args.Equal(i.queryArgs()))
}

var res value.Value

func TestInterpreter_VisitLookup(t *testing.T) {
	// lookups on non dicts all fail
	for _, tree := range ast.TestExamples {
		if _, ok := tree.(ast.Dict); !ok {
			testError(t, ast.Lookup{On: tree, Property: "hi"})
		}
	}
	// and we get error on empty dict too
	testError(t, ast.Lookup{On: ast.Dict{Values: map[string]ast.Ast{}}, Property: "hi"})

	// dict with just one element works only if property is same
	d := ast.Dict{Values: map[string]ast.Ast{"hi": ast.MakeDouble(3.4)}}
	testValid(t, ast.Lookup{On: d, Property: "hi"}, value.Double(3.4))
	testError(t, ast.Lookup{On: d, Property: "bye"})

	// and so does a multi-element list with mixed types and nesting
	nested := ast.Dict{Values: map[string]ast.Ast{
		"hi":     ast.MakeDouble(4.4),
		"bye":    ast.MakeBool(false),
		"nested": d,
	}}
	testValid(t, ast.Lookup{On: ast.Lookup{On: nested, Property: "nested"}, Property: "hi"}, value.Double(3.4))
	testValid(t, ast.Lookup{On: nested, Property: "hi"}, value.Double(4.4))
}

func TestFailEmptyOperand(t *testing.T) {
	i := getInterpreter(nil, value.Dict{})
	_, err := i.VisitOpcall([]ast.Ast{}, []string{}, "std", "set", ast.Dict{})
	require.Error(t, err)
}

func TestVisitOpCallMultiop(t *testing.T) {
	i := getInterpreter(nil, value.Dict{})
	q := ast.OpCall{
		Operands: []ast.Ast{
			ast.List{
				Values: []ast.Ast{
					ast.MakeInt(5),
					ast.MakeInt(9),
				},
			},
			ast.List{
				Values: []ast.Ast{
					ast.MakeInt(3),
					ast.MakeInt(4),
				},
			},
		},
		Vars:      []string{"a", "b"},
		Namespace: "std",
		Name:      "map",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"to": ast.Binary{
				Op:    "+",
				Left:  ast.Var{Name: "a"},
				Right: ast.Var{Name: "b"},
			},
		}},
	}
	out, err := q.AcceptValue(i)
	require.NoError(t, err)
	assert.Equal(t, value.NewList(value.Int(8), value.Int(13)), out)
}

func getOpCallQuery() ast.Ast {
	return ast.OpCall{
		Operands: []ast.Ast{ast.OpCall{
			Operands:  []ast.Ast{ast.Var{Name: "table"}},
			Vars:      []string{"at"},
			Namespace: "std",
			Name:      "filter",
			Kwargs: ast.Dict{Values: map[string]ast.Ast{
				"where": ast.Binary{
					Left:  ast.Lookup{On: ast.Var{Name: "at"}, Property: "hi"},
					Op:    ">=",
					Right: ast.MakeInt(2),
				},
			}},
		}},
		Vars:      []string{"at"},
		Namespace: "std",
		Name:      "set",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"field": ast.MakeString("key"),
			"value": ast.List{Values: []ast.Ast{ast.Lookup{
				On:       ast.Var{Name: "at"},
				Property: "bye",
			}}},
		}},
	}
}

func TestInterpreter_VisitIfelse(t *testing.T) {
	testValid(t, ast.IfElse{
		Condition: ast.MakeBool(true),
		ThenDo:    ast.MakeInt(2),
		ElseDo:    ast.MakeInt(9),
	}, value.Int(2))
	testValid(t, ast.IfElse{
		Condition: ast.MakeBool(false),
		ThenDo:    ast.MakeInt(1),
		ElseDo:    ast.MakeInt(8),
	}, value.Int(8))

	testError(t, ast.IfElse{
		Condition: ast.MakeInt(10),
		ThenDo:    ast.MakeInt(11),
		ElseDo:    ast.MakeInt(12),
	})

	testDualBranchEvaluation(t)
}

// Test that only one of the then/else branches is evaluated
func testDualBranchEvaluation(t *testing.T) {
	i := getInterpreter(nil, value.Dict{})

	// Only the then branch should be evaluated
	ifelse1 := ast.IfElse{
		Condition: ast.MakeBool(true),
		ThenDo:    ast.Statement{Name: "x", Body: ast.MakeInt(4)},
		ElseDo:    ast.Statement{Name: "y", Body: ast.MakeInt(5)},
	}
	ret, err := ifelse1.AcceptValue(i)
	assert.NoError(t, err)
	assert.Equal(t, value.Int(4), ret)

	x := ast.Var{Name: "x"}
	ret, err = x.AcceptValue(i)
	assert.NoError(t, err)
	assert.Equal(t, value.Int(4), ret)

	y := ast.Var{Name: "y"}
	_, err = y.AcceptValue(i)
	assert.Error(t, err)

	// Only the else branch should be evaluated
	ifelse2 := ast.IfElse{
		Condition: ast.MakeBool(false),
		ThenDo:    ast.Statement{Name: "a", Body: ast.MakeString("abc")},
		ElseDo:    ast.Statement{Name: "b", Body: ast.MakeString("xyz")},
	}
	ret, err = ifelse2.AcceptValue(i)
	assert.NoError(t, err)
	assert.Equal(t, value.String("xyz"), ret)

	a := ast.Var{Name: "a"}
	_, err = a.AcceptValue(i)
	assert.Error(t, err)

	b := ast.Var{Name: "b"}
	ret, err = b.AcceptValue(i)
	assert.NoError(t, err)
	assert.Equal(t, value.String("xyz"), ret)
}

func TestInterpreter_VisitVarClone(t *testing.T) {
	i := getInterpreter(nil, value.Dict{})
	// checking if var actually clones
	// define a variable and perform an opcall that changes the input
	// check if output is as expected and original variable was not changed
	query := ast.Query{Statements: []ast.Statement{
		{
			Name: "x",
			Body: ast.Dict{Values: map[string]ast.Ast{"key": ast.MakeInt(0)}},
		},
		{
			Name: "y",
			Body: ast.OpCall{
				Namespace: "std",
				Name:      "set",
				Operands:  []ast.Ast{ast.List{Values: []ast.Ast{ast.Var{Name: "x"}}}},
				Vars:      nil,
				Kwargs: ast.Dict{Values: map[string]ast.Ast{
					"field": ast.MakeString("col"),
					"value": ast.MakeString("new"),
				}},
			},
		},
		{
			Name: "",
			Body: ast.Dict{Values: map[string]ast.Ast{
				"x": ast.Var{Name: "x"},
				"y": ast.Var{Name: "y"},
			}},
		},
	}}
	expectedX := value.NewDict(map[string]value.Value{"key": value.Int(0)})
	expectedY := value.NewList(value.NewDict(map[string]value.Value{
		"col": value.String("new"),
		"key": value.Int(0),
	}))

	found, err := query.AcceptValue(i)
	assert.NoError(t, err)
	asDict, ok := found.(value.Dict)
	assert.True(t, ok)
	assert.True(t, expectedX.Equal(asDict.GetUnsafe("x")))
	assert.True(t, expectedY.Equal(asDict.GetUnsafe("y")))
}
