package interpreter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/engine/ast"
	"fennel/engine/operators"
	"fennel/lib/value"
	_ "fennel/opdefs"
)

func getInterpreter() Interpreter {
	return NewInterpreter(map[string]interface{}{})
}

func testValid(t *testing.T, node ast.Ast, expected value.Value) {
	i := getInterpreter()
	ret, err := node.AcceptValue(i)
	assert.NoError(t, err)
	assert.Equal(t, expected, ret)
}

func testError(t *testing.T, node ast.Ast) {
	i := getInterpreter()
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

func TestInterpreter_VisitList(t *testing.T) {
	// Empty list works
	testValid(t, ast.List{Values: []ast.Ast{}}, value.List{})

	// list with just one element works
	l := value.NewList([]value.Value{value.Double(3.4)})
	testValid(t, &ast.List{Values: []ast.Ast{ast.MakeDouble(3.4)}}, l)
	// and so does a multi-element list with mixed types
	l = value.NewList([]value.Value{value.Double(3.4), value.Bool(false), value.String("hi")})
	testValid(t, &ast.List{Values: []ast.Ast{ast.MakeDouble(3.4), ast.MakeBool(false), ast.MakeString("hi")}}, l)
}

func TestInterpreter_VisitDict(t *testing.T) {
	// Empty dict works
	testValid(t, ast.Dict{Values: map[string]ast.Ast{}}, value.Dict{})

	// dict with just one element works
	d, _ := value.NewDict(map[string]value.Value{"hi": value.Double(3.4)})
	testValid(t, ast.Dict{Values: map[string]ast.Ast{"hi": ast.MakeDouble(3.4)}}, d)
	// and so does a multi-element list with mixed types and nesting
	nested, _ := value.NewDict(map[string]value.Value{
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

func TestInterpreter_VisitTable(t *testing.T) {
	astrow1 := ast.Dict{
		Values: map[string]ast.Ast{
			"a.inner": ast.MakeInt(3),
			"b":       ast.MakeString("hi"),
		},
	}
	astrow2 := ast.Dict{
		Values: map[string]ast.Ast{
			"a.inner": ast.MakeInt(5),
			"b":       ast.MakeString("bye"),
		},
	}
	astrow3 := ast.Dict{
		Values: map[string]ast.Ast{
			"b":       ast.MakeString("hello"),
			"a.inner": ast.MakeInt(3),
		},
	}
	row1, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(3),
		"b":       value.String("hi"),
	})
	row2, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(5),
		"b":       value.String("bye"),
	})
	row3, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(3),
		"b":       value.String("hello"),
	})

	// creating empty table works
	testValid(t, ast.Table{Inner: ast.List{}}, value.NewTable())
	// so does with one row
	t1 := value.Table{}
	t1.Append(row1)
	testValid(t, ast.Table{Inner: ast.List{Values: []ast.Ast{astrow1}}}, t1)

	// and same with multiple rows including nested rows
	t1.Append(row2)
	t1.Append(row3)
	testValid(t, ast.Table{Inner: ast.List{Values: []ast.Ast{astrow1, astrow2, astrow3}}}, t1)
}

func TestInterpreter_VisitTableErrors(t *testing.T) {

	// visiting table with non-list or non-table doesn't work
	testError(t, ast.Table{Inner: ast.MakeInt(123)})
	testError(t, ast.Table{Inner: ast.MakeString("123")})
	testError(t, ast.Table{Inner: ast.Dict{Values: map[string]ast.Ast{}}})

	// even for lists, it only works when its items are dicts
	testError(t, ast.Table{Inner: ast.List{Values: []ast.Ast{ast.MakeInt(123)}}})
	testError(t, ast.Table{Inner: ast.List{Values: []ast.Ast{ast.Dict{Values: map[string]ast.Ast{}}, ast.MakeInt(123)}}})

	// and even then, it only works when they all have the same schema
	testError(t, &ast.Table{Inner: ast.List{Values: []ast.Ast{ast.Dict{Values: map[string]ast.Ast{
		"a": ast.MakeInt(123),
		"b": ast.MakeBool(true),
	}},
		ast.Dict{Values: map[string]ast.Ast{}}}}},
	)

	testError(t, ast.Table{Inner: ast.List{Values: []ast.Ast{
		ast.Dict{Values: map[string]ast.Ast{
			"a": ast.MakeInt(123),
			"b": ast.MakeBool(true),
		}},
		ast.Dict{Values: map[string]ast.Ast{
			"a": ast.MakeInt(123),
			"c": ast.MakeBool(true),
		}},
	}}})

	// same for nested
	testError(t, &ast.Table{Inner: ast.List{Values: []ast.Ast{
		ast.Dict{Values: map[string]ast.Ast{
			"a": ast.MakeInt(123),
			"b": ast.Dict{
				Values: map[string]ast.Ast{
					"inner": ast.MakeInt(123),
				},
			},
		}},
		ast.Dict{Values: map[string]ast.Ast{
			"a": ast.MakeInt(123),
			"c": ast.MakeBool(false),
		}},
	}}})
}

func TestInterpreter_VisitOpcall(t *testing.T) {
	astrow1 := ast.Dict{
		Values: map[string]ast.Ast{
			"a.inner": ast.MakeInt(3),
			"b":       ast.MakeString("hi"),
		},
	}
	astrow2 := ast.Dict{
		Values: map[string]ast.Ast{
			"a.inner": ast.MakeInt(5),
			"b":       ast.MakeString("bye"),
		},
	}
	astrow3 := ast.Dict{
		Values: map[string]ast.Ast{
			"b":       ast.MakeString("hello"),
			"a.inner": ast.MakeInt(3),
		},
	}
	astTable := &ast.Table{Inner: ast.List{Values: []ast.Ast{astrow1, astrow2, astrow3}}}
	row1, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(3),
		"b":       value.String("hi"),
	})
	row2, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(5),
		"b":       value.String("bye"),
	})
	row3, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(3),
		"b":       value.String("hello"),
	})
	table := value.NewTable()
	table.Append(row1)
	table.Append(row2)
	table.Append(row3)

	// we get all values back if where is true
	kwargs := ast.Dict{
		Values: map[string]ast.Ast{
			"where": ast.MakeBool(true),
		},
	}
	testValid(t, ast.OpCall{
		Operand:   astTable,
		Namespace: "std",
		Name:      "filter",
		Kwargs:    kwargs,
	}, table)

	// and we get nothing when filter is passed as "false"
	kwargs = ast.Dict{
		Values: map[string]ast.Ast{
			"where": ast.MakeBool(false),
		},
	}
	testValid(t, ast.OpCall{
		Operand:   astTable,
		Namespace: "std",
		Name:      "filter",
		Kwargs:    kwargs,
	}, value.NewTable())

	// and if where is more specific, that works too
	kwargs = ast.Dict{
		Values: map[string]ast.Ast{
			"where": ast.Binary{
				Left:  ast.Lookup{ast.At{}, "a.inner"},
				Right: ast.MakeInt(3),
				Op:    "==",
			},
		},
	}
	expected := value.NewTable()
	expected.Append(row1)
	expected.Append(row3)
	testValid(t, ast.OpCall{
		Operand:   astTable,
		Namespace: "std",
		Name:      "filter",
		Kwargs:    kwargs,
	}, expected)
}

func TestInterpreter_VisitOpcall2(t *testing.T) {
	// here we create nested opcall that contain both static/contextual kwargs
	base := value.NewTable()
	row1 := value.Dict{"hi": value.Int(1), "bye": value.Double(1)}
	row2 := value.Dict{"hi": value.Int(2), "bye": value.Double(2)}
	row3 := value.Dict{"hi": value.Int(3), "bye": value.Double(3)}
	assert.NoError(t, base.Append(row1))
	assert.NoError(t, base.Append(row2))
	assert.NoError(t, base.Append(row3))
	i := getInterpreter()
	i.SetVar("table", base)
	query := getOpCallQuery()
	res, err := query.AcceptValue(i)
	assert.NoError(t, err)
	expected := value.NewTable()
	assert.NoError(t, expected.Append(value.Dict{"hi": value.Int(2), "bye": value.Double(2), "key": value.List{value.Double(2)}}))
	assert.NoError(t, expected.Append(value.Dict{"hi": value.Int(3), "bye": value.Double(3), "key": value.List{value.Double(3)}}))
	assert.Equal(t, expected, res)
}

func TestInterpreter_QueryArgs(t *testing.T) {
	i := getInterpreter()
	// initially nothing
	assert.Equal(t, value.Dict{}, i.QueryArgs())
	args := value.Dict{"x": value.Int(1)}
	assert.NoError(t, i.SetQueryArgs(args))
	assert.Equal(t, args, i.QueryArgs())
}

var res value.Value

func benchmarkInterpreter_VisitOpcall(numRows int, b *testing.B) {
	table := value.NewTable()
	for i := 0; i < numRows; i++ {
		row := value.Dict{"hi": value.Int(i), "bye": value.Double(i)}
		table.Append(row)
	}
	evaler := getInterpreter()
	evaler.SetVar("table", table)
	query := getOpCallQuery()
	for i := 0; i < b.N; i++ {
		res, _ = query.AcceptValue(evaler)
	}
}

func BenchmarkInterpreter_VisitOpcall100(b *testing.B) { benchmarkInterpreter_VisitOpcall(100, b) }
func BenchmarkInterpreter_VisitOpcall1K(b *testing.B)  { benchmarkInterpreter_VisitOpcall(1000, b) }
func BenchmarkInterpreter_VisitOpcall10K(b *testing.B) { benchmarkInterpreter_VisitOpcall(10000, b) }

func TestInterpreter_VisitAt(t *testing.T) {
	testError(t, ast.At{})
	values := []value.Value{
		value.Int(5),
		value.Bool(false),
		value.List([]value.Value{value.Double(3.4)}),
	}
	// value of at is just whatever is set to be @
	for _, v := range values {
		i := getInterpreter()
		err := i.env.Define("@", v)
		assert.NoError(t, err)
		ret, err := ast.At{}.AcceptValue(i)
		assert.NoError(t, err)
		assert.Equal(t, v, ret)
	}
}

func TestInterpreter_VisitLookup(t *testing.T) {
	// lookups on non dicts all fail
	for _, tree := range ast.TestExamples {
		if _, ok := tree.(ast.Dict); !ok {
			testError(t, ast.Lookup{tree, "hi"})
		}
	}
	// and we get error on empty dict too
	testError(t, ast.Lookup{On: ast.Dict{Values: map[string]ast.Ast{}}, Property: "hi"})

	// dict with just one element works only if property is same
	d := ast.Dict{map[string]ast.Ast{"hi": ast.MakeDouble(3.4)}}
	testValid(t, ast.Lookup{d, "hi"}, value.Double(3.4))
	testError(t, ast.Lookup{d, "bye"})

	// and so does a multi-element list with mixed types and nesting
	nested := ast.Dict{map[string]ast.Ast{
		"hi":     ast.MakeDouble(4.4),
		"bye":    ast.MakeBool(false),
		"nested": d,
	}}
	testValid(t, ast.Lookup{ast.Lookup{nested, "nested"}, "hi"}, value.Double(3.4))
	testValid(t, ast.Lookup{nested, "hi"}, value.Double(4.4))
}

func TestInterpreter_SetVar(t *testing.T) {
	i := getInterpreter()
	name := "key"
	val := value.Int(4)
	_, err := i.env.Lookup(name)
	assert.Error(t, err)

	assert.NoError(t, i.SetVar(name, val))
	found, err := i.env.Lookup(name)
	assert.NoError(t, err)
	assert.Equal(t, val, found)
	assert.Error(t, i.SetVar(name, val))
	found, err = i.env.Lookup(name)
	assert.NoError(t, err)
	assert.Equal(t, val, found)
}

func getOpCallQuery() ast.Ast {
	return ast.OpCall{
		Operand: ast.OpCall{
			Operand:   ast.Var{Name: "table"},
			Namespace: "std",
			Name:      "filter",
			Kwargs: ast.Dict{Values: map[string]ast.Ast{
				"where": ast.Binary{
					Left:  ast.Lookup{On: ast.At{}, Property: "hi"},
					Op:    ">=",
					Right: ast.MakeInt(2),
				},
			}},
		},
		Namespace: "std",
		Name:      "addField",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"name": ast.MakeString("key"),
			"value": ast.List{Values: []ast.Ast{ast.Lookup{
				On:       ast.At{},
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
	i := getInterpreter()

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

func TestInterpreter_VisitOpcall3(t *testing.T) {
	// first register the test operation
	operators.Register(&testOpInit{})
	// then create an ast that uses this op
	query := ast.OpCall{
		Operand:   ast.Var{Name: "table"},
		Namespace: "test",
		Name:      "op",
		Kwargs:    ast.Dict{},
	}
	table := value.NewTable()
	table.Append(value.Dict{"x": value.Int(1)})
	nonhi := "hello"
	i := NewInterpreter(map[string]interface{}{
		"__teststruct__": testNonValue{hi: nonhi},
	})
	i.SetQueryArgs(value.Dict{"num": value.Int(41)})
	assert.NoError(t, i.SetVar("table", table))
	out, err := query.AcceptValue(i)
	assert.NoError(t, err)
	outtable := out.(value.Table)
	rows := outtable.Pull()
	assert.Len(t, rows, 1)
	assert.Equal(t, value.Dict{"x": value.Int(1), "num": value.Int(41), "nonhi": value.String(nonhi)}, rows[0])
}

func TestInterpreter_VisitOpcall4(t *testing.T) {
	operators.Register(testOpDefault{})
	query := ast.OpCall{
		Operand:   ast.Var{Name: "table"},
		Namespace: "test",
		Name:      "testop",
		Kwargs:    ast.Dict{},
	}
	table := value.NewTable()
	table.Append(value.Dict{})
	i := getInterpreter()
	i.SetQueryArgs(value.Dict{})
	assert.NoError(t, i.SetVar("table", table))
	out, err := query.AcceptValue(i)
	assert.NoError(t, err)
	outtable := out.(value.Table)
	rows := outtable.Pull()
	assert.Len(t, rows, 1)
	assert.Equal(t, value.Dict{"contextual": value.Int(41), "static": value.Int(7)}, rows[0])
}

type testOpDefault struct{}

func (t testOpDefault) Init(args value.Dict, bootargs map[string]interface{}) error { return nil }
func (t testOpDefault) Apply(kwargs value.Dict, in operators.InputIter, out *value.Table) error {
	for in.HasMore() {
		row, context, _ := in.Next()
		row["contextual"] = context["contextual"]
		row["static"] = kwargs["static"]
		out.Append(row)
	}
	return nil
}

func (t testOpDefault) Signature() *operators.Signature {
	return operators.NewSignature(t, "test", "testop").
		Param("contextual", value.Types.Int, false, true, value.Int(41)).
		Param("static", value.Types.Int, true, true, value.Int(7))
}

var _ operators.Operator = testOpDefault{}

type testOpInit struct {
	num value.Int
	non testNonValue
}
type testNonValue struct {
	hi string
}

var _ operators.Operator = &testOpInit{}

func (top *testOpInit) Init(args value.Dict, bootargs map[string]interface{}) error {
	// take one arg from args and one from bootarg to verify that init is working
	num, ok := args["num"]
	if !ok {
		return fmt.Errorf("num not passed")
	}
	top.num = num.(value.Int)
	top.non = bootargs["__teststruct__"].(testNonValue)
	return nil
}

func (top *testOpInit) Apply(kwargs value.Dict, in operators.InputIter, out *value.Table) error {
	for in.HasMore() {
		row, _, _ := in.Next()
		row["num"] = top.num
		row["nonhi"] = value.String(top.non.hi)
		out.Append(row)
	}
	return nil
}

func (top *testOpInit) Signature() *operators.Signature {
	return operators.NewSignature(top, "test", "op")
}
