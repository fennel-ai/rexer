package interpreter

import (
	"fennel/engine/ast"
	"fennel/lib/value"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getInterpreter() Interpreter {
	return Interpreter{
		NewEnv(),
	}
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
	l, _ := value.NewList([]value.Value{value.Double(3.4)})
	testValid(t, &ast.List{Values: []ast.Ast{ast.MakeDouble(3.4)}}, l)
	// and so does a multi-element list with mixed types
	l, _ = value.NewList([]value.Value{value.Double(3.4), value.Bool(false), value.String("hi")})
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
			"a": ast.Dict{
				Values: map[string]ast.Ast{
					"inner": ast.MakeInt(5),
				}},
			"b": ast.MakeString("bye"),
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
			"a": ast.Dict{
				Values: map[string]ast.Ast{
					"inner": ast.MakeInt(5),
				}},
			"b": ast.MakeString("bye"),
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
}
