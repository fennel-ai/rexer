package interpreter

import (
	"fennel/engine/ast"
	"fennel/value"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getInterpreter() Interpreter {
	return Interpreter{
		NewEnv(),
	}
}

func testValid(t *testing.T, node ast.AstNode, expected value.Value) {
	i := getInterpreter()
	ret, err := node.AcceptValue(i)
	assert.NoError(t, err)
	assert.Equal(t, expected, ret)
}

func testError(t *testing.T, node ast.AstNode) {
	i := getInterpreter()
	_, err := node.AcceptValue(i)
	assert.Error(t, err)
}

func makeInt(i int32) *ast.Atom {
	return &ast.Atom{
		Inner: &ast.Atom_Int{
			Int: i,
		},
	}
}

func makeDouble(d float64) *ast.Atom {
	return &ast.Atom{
		Inner: &ast.Atom_Double{
			Double: d,
		},
	}
}

func makeString(s string) *ast.Atom {
	return &ast.Atom{
		Inner: &ast.Atom_String_{
			String_: s,
		},
	}
}

func makeBool(b bool) *ast.Atom {
	return &ast.Atom{
		Inner: &ast.Atom_Bool{
			Bool: b,
		},
	}
}

func TestInterpreter_VisitAtom(t *testing.T) {
	testValid(t, makeInt(123), value.Int(123))
	testValid(t, makeInt(-123), value.Int(-123))
	// possible to parse double without decimal
	testValid(t, makeDouble(123), value.Double(123.0))
	testValid(t, makeDouble(123.3), value.Double(123.3))
	testValid(t, makeDouble(-123.3), value.Double(-123.3))

	testValid(t, makeBool(true), value.Bool(true))
	testValid(t, makeBool(false), value.Bool(false))

	testValid(t, makeString("hi"), value.String("hi"))
	testValid(t, makeString("false"), value.String("false"))
	testValid(t, makeString("3.2"), value.String("3.2"))
}

func TestInterpreter_VisitBinary(t *testing.T) {
	testValid(t, &ast.Binary{
		Left:  &ast.Ast{Node: &ast.Ast_Atom{Atom: makeInt(5)}},
		Op:    "+",
		Right: &ast.Ast{Node: &ast.Ast_Atom{Atom: makeInt(8)}},
	}, value.Int(13))

	// and errors are propagated through type errors.
	testError(t, &ast.Binary{
		Left:  &ast.Ast{Node: &ast.Ast_Atom{Atom: makeInt(5)}},
		Op:    "*",
		Right: &ast.Ast{Node: &ast.Ast_Atom{Atom: makeBool(false)}},
	})
}

func TestInterpreter_VisitList(t *testing.T) {
	// Empty list works
	testValid(t, &ast.List{Elems: []*ast.Ast{}}, value.List{})

	// list with just one element works
	l, _ := value.NewList([]value.Value{value.Double(3.4)})
	testValid(t, &ast.List{Elems: []*ast.Ast{
		ast.MakeAst(makeDouble(3.4)),
	}}, l)
	// and so does a multi-element list with mixed types
	l, _ = value.NewList([]value.Value{value.Double(3.4), value.Bool(false), value.String("hi")})
	testValid(t, &ast.List{Elems: []*ast.Ast{
		ast.MakeAst(makeDouble(3.4)),
		ast.MakeAst(makeBool(false)),
		ast.MakeAst(makeString("hi")),
	}}, l)
}

func TestInterpreter_VisitDict(t *testing.T) {
	// Empty dict works
	testValid(t, &ast.Dict{Values: map[string]*ast.Ast{}}, value.Dict{})

	// dict with just one element works
	d, _ := value.NewDict(map[string]value.Value{"hi": value.Double(3.4)})
	testValid(t, &ast.Dict{Values: map[string]*ast.Ast{
		"hi": ast.MakeAst(makeDouble(3.4)),
	}}, d)
	// and so does a multi-element list with mixed types and nesting
	nested, _ := value.NewDict(map[string]value.Value{
		"hi":     value.Double(3.4),
		"bye":    value.Bool(false),
		"nested": d,
	})
	testValid(t, &ast.Dict{Values: map[string]*ast.Ast{
		"hi":  ast.MakeAst(makeDouble(3.4)),
		"bye": ast.MakeAst(makeBool(false)),
		"nested": ast.MakeAst(&ast.Dict{
			Values: map[string]*ast.Ast{
				"hi": ast.MakeAst(makeDouble(3.4)),
			}}),
	}}, nested)
}

func TestInterpreter_VisitStatement(t *testing.T) {
	s := &ast.Statement{Name: "var", Body: &ast.Ast{Node: &ast.Ast_Atom{Atom: makeBool(false)}}}
	testValid(t, s, value.Bool(false))

	// same happens if no name is passed
	s = &ast.Statement{Name: "", Body: &ast.Ast{Node: &ast.Ast_Atom{Atom: makeBool(false)}}}
	testValid(t, s, value.Bool(false))
}

func TestInterpreter_VisitTable(t *testing.T) {
	astrow1 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"a.inner": ast.MakeAst(makeInt(3)),
			"b":       ast.MakeAst(makeString("hi")),
		},
	}
	astrow2 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"a": ast.MakeAst(&ast.Dict{
				Values: map[string]*ast.Ast{
					"inner": ast.MakeAst(makeInt(5)),
				}}),
			"b": ast.MakeAst(makeString("bye")),
		},
	}
	astrow3 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"b":       ast.MakeAst(makeString("hello")),
			"a.inner": ast.MakeAst(makeInt(3)),
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
	testValid(t, &ast.Table{
		Inner: ast.MakeAst(&ast.List{}),
	}, value.NewTable())

	// so does with one astrow
	t1 := value.Table{}
	t1.Append(row1)
	testValid(t, &ast.Table{
		Inner: ast.MakeAst(&ast.List{
			Elems: []*ast.Ast{ast.MakeAst(astrow1)},
		}),
	}, t1)

	// and same with multiple rows including nested rows
	t1.Append(row2)
	t1.Append(row3)
	testValid(t, &ast.Table{
		Inner: ast.MakeAst(&ast.List{
			Elems: []*ast.Ast{
				ast.MakeAst(astrow1),
				ast.MakeAst(astrow2),
				ast.MakeAst(astrow3),
			},
		}),
	}, t1)
}

func TestInterpreter_VisitTableErrors(t *testing.T) {

	// visiting table with non-list or non-table doesn't work
	testError(t, &ast.Table{Inner: ast.MakeAst(makeInt(123))})
	testError(t, &ast.Table{Inner: ast.MakeAst(makeString("123"))})
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{}})})

	// even for lists, it only works when its items are dicts
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.List{Elems: []*ast.Ast{
		ast.MakeAst(makeInt(123)),
	}})})
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.List{Elems: []*ast.Ast{
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{}}),
		ast.MakeAst(makeInt(123)),
	}})})

	// and even then, it only works when they all have the same schema
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.List{Elems: []*ast.Ast{
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{
			"a": ast.MakeAst(makeInt(123)),
			"b": ast.MakeAst(makeBool(true)),
		}}),
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{}}),
	}})})
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.List{Elems: []*ast.Ast{
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{
			"a": ast.MakeAst(makeInt(123)),
			"b": ast.MakeAst(makeBool(true)),
		}}),
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{
			"a": ast.MakeAst(makeInt(123)),
			"c": ast.MakeAst(makeBool(true)),
		}}),
	}})})

	// same for nested
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.List{Elems: []*ast.Ast{
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{
			"a": ast.MakeAst(makeInt(123)),
			"b": ast.MakeAst(&ast.Dict{
				Values: map[string]*ast.Ast{
					"inner": ast.MakeAst(makeInt(123)),
				},
			}),
		}}),
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{
			"a": ast.MakeAst(makeInt(123)),
			"c": ast.MakeAst(makeBool(false)),
		}}),
	}})})
}

func TestInterpreter_VisitOpcall(t *testing.T) {
	astrow1 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"a.inner": ast.MakeAst(makeInt(3)),
			"b":       ast.MakeAst(makeString("hi")),
		},
	}
	astrow2 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"a": ast.MakeAst(&ast.Dict{
				Values: map[string]*ast.Ast{
					"inner": ast.MakeAst(makeInt(5)),
				}}),
			"b": ast.MakeAst(makeString("bye")),
		},
	}
	astrow3 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"b":       ast.MakeAst(makeString("hello")),
			"a.inner": ast.MakeAst(makeInt(3)),
		},
	}
	astTable := &ast.Table{
		Inner: ast.MakeAst(&ast.List{
			Elems: []*ast.Ast{
				ast.MakeAst(astrow1),
				ast.MakeAst(astrow2),
				ast.MakeAst(astrow3),
			},
		}),
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
	table := value.NewTable()
	table.Append(row1)
	table.Append(row2)
	table.Append(row3)

	kwargs := &ast.Dict{
		Values: map[string]*ast.Ast{
			"where": ast.MakeAst(makeBool(true)),
		},
	}
	testValid(t, &ast.OpCall{
		Operand:   ast.MakeAst(astTable),
		Namespace: "std",
		Name:      "filter",
		Kwargs:    kwargs,
	}, table)

	// and we get nothing when filter is passed as "false"
	kwargs = &ast.Dict{
		Values: map[string]*ast.Ast{
			"where": ast.MakeAst(makeBool(false)),
		},
	}
	testValid(t, &ast.OpCall{
		Operand:   ast.MakeAst(astTable),
		Namespace: "std",
		Name:      "filter",
		Kwargs:    kwargs,
	}, value.NewTable())
}
