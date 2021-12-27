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

func makeAtom(atomtype ast.AtomType, lexeme string) *ast.Atom {
	return &ast.Atom{AtomType: atomtype, Lexeme: lexeme}
}

func TestInterpreter_VisitAtom(t *testing.T) {
	testValid(t, makeAtom(ast.AtomType_INT, "123"), value.Int(123))
	testValid(t, makeAtom(ast.AtomType_INT, "-123"), value.Int(-123))
	// possible to parse double without decimal
	testValid(t, makeAtom(ast.AtomType_DOUBLE, "123"), value.Double(123.0))
	testValid(t, makeAtom(ast.AtomType_DOUBLE, "123.3"), value.Double(123.3))
	testValid(t, makeAtom(ast.AtomType_DOUBLE, "-123.3"), value.Double(-123.3))

	testValid(t, makeAtom(ast.AtomType_BOOL, "true"), value.Bool(true))
	testValid(t, makeAtom(ast.AtomType_BOOL, "false"), value.Bool(false))
	// possible to parse bools from ints
	testValid(t, makeAtom(ast.AtomType_BOOL, "1"), value.Bool(true))
	testValid(t, makeAtom(ast.AtomType_BOOL, "0"), value.Bool(false))

	testValid(t, makeAtom(ast.AtomType_STRING, "hi"), value.String("hi"))
	testValid(t, makeAtom(ast.AtomType_STRING, "false"), value.String("false"))
	testValid(t, makeAtom(ast.AtomType_STRING, "3.2"), value.String("3.2"))

	// invalid checks
	testError(t, makeAtom(ast.AtomType_INT, "true"))
	testError(t, makeAtom(ast.AtomType_INT, "123.0"))
	testError(t, makeAtom(ast.AtomType_INT, "hi"))

	testError(t, makeAtom(ast.AtomType_DOUBLE, "true"))
	testError(t, makeAtom(ast.AtomType_DOUBLE, "hi"))

	testError(t, makeAtom(ast.AtomType_BOOL, "5"))
	testError(t, makeAtom(ast.AtomType_BOOL, "3.2"))
	testError(t, makeAtom(ast.AtomType_BOOL, "hi"))
}

func TestInterpreter_VisitBinary(t *testing.T) {
	testValid(t, &ast.Binary{
		Left:  &ast.Ast{Node: &ast.Ast_Atom{Atom: makeAtom(ast.AtomType_INT, "5")}},
		Op:    "+",
		Right: &ast.Ast{Node: &ast.Ast_Atom{Atom: makeAtom(ast.AtomType_INT, "8")}},
	}, value.Int(13))

	// and errors are propagated too - both parse errors...
	testError(t, &ast.Binary{
		Left:  &ast.Ast{Node: &ast.Ast_Atom{Atom: makeAtom(ast.AtomType_INT, "5")}},
		Op:    "*",
		Right: &ast.Ast{Node: &ast.Ast_Atom{Atom: makeAtom(ast.AtomType_INT, "false")}},
	})
	// ...and type errors
	testError(t, &ast.Binary{
		Left:  &ast.Ast{Node: &ast.Ast_Atom{Atom: makeAtom(ast.AtomType_INT, "5")}},
		Op:    "*",
		Right: &ast.Ast{Node: &ast.Ast_Atom{Atom: makeAtom(ast.AtomType_BOOL, "false")}},
	})
}

func TestInterpreter_VisitList(t *testing.T) {
	// Empty list works
	testValid(t, &ast.List{Elems: []*ast.Ast{}}, value.List{})

	// list with just one element works
	l, _ := value.NewList([]value.Value{value.Double(3.4)})
	testValid(t, &ast.List{Elems: []*ast.Ast{
		ast.MakeAst(makeAtom(ast.AtomType_DOUBLE, "3.4")),
	}}, l)
	// and so does a multi-element list with mixed types
	l, _ = value.NewList([]value.Value{value.Double(3.4), value.Bool(false), value.String("hi")})
	testValid(t, &ast.List{Elems: []*ast.Ast{
		ast.MakeAst(makeAtom(ast.AtomType_DOUBLE, "3.4")),
		ast.MakeAst(makeAtom(ast.AtomType_BOOL, "false")),
		ast.MakeAst(makeAtom(ast.AtomType_STRING, "hi")),
	}}, l)
}

func TestInterpreter_VisitDict(t *testing.T) {
	// Empty dict works
	testValid(t, &ast.Dict{Values: map[string]*ast.Ast{}}, value.Dict{})

	// dict with just one element works
	d, _ := value.NewDict(map[string]value.Value{"hi": value.Double(3.4)})
	testValid(t, &ast.Dict{Values: map[string]*ast.Ast{
		"hi": ast.MakeAst(makeAtom(ast.AtomType_DOUBLE, "3.4")),
	}}, d)
	// and so does a multi-element list with mixed types and nesting
	nested, _ := value.NewDict(map[string]value.Value{
		"hi":     value.Double(3.4),
		"bye":    value.Bool(false),
		"nested": d,
	})
	testValid(t, &ast.Dict{Values: map[string]*ast.Ast{
		"hi":  ast.MakeAst(makeAtom(ast.AtomType_DOUBLE, "3.4")),
		"bye": ast.MakeAst(makeAtom(ast.AtomType_BOOL, "false")),
		"nested": ast.MakeAst(&ast.Dict{
			Values: map[string]*ast.Ast{
				"hi": ast.MakeAst(makeAtom(ast.AtomType_DOUBLE, "3.4")),
			}}),
	}}, nested)
}

func TestInterpreter_VisitStatement(t *testing.T) {
	s := &ast.Statement{Name: "var", Body: &ast.Ast{Node: &ast.Ast_Atom{Atom: &ast.Atom{AtomType: ast.AtomType_BOOL, Lexeme: "false"}}}}
	testValid(t, s, value.Bool(false))

	// same happens if no name is passed
	s = &ast.Statement{Name: "", Body: &ast.Ast{Node: &ast.Ast_Atom{Atom: &ast.Atom{AtomType: ast.AtomType_BOOL, Lexeme: "false"}}}}
	testValid(t, s, value.Bool(false))
}

func TestInterpreter_VisitTable(t *testing.T) {
	astrow1 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"a.inner": ast.MakeAst(makeAtom(ast.AtomType_INT, "3")),
			"b":       ast.MakeAst(makeAtom(ast.AtomType_STRING, "hi")),
		},
	}
	astrow2 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"a": ast.MakeAst(&ast.Dict{
				Values: map[string]*ast.Ast{
					"inner": ast.MakeAst(makeAtom(ast.AtomType_INT, "5")),
				}}),
			"b": ast.MakeAst(makeAtom(ast.AtomType_STRING, "bye")),
		},
	}
	astrow3 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"b":       ast.MakeAst(makeAtom(ast.AtomType_STRING, "hello")),
			"a.inner": ast.MakeAst(makeAtom(ast.AtomType_INT, "3")),
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

	// if there is error in processing inner, that error is propagated
	testError(t, &ast.Table{
		Inner: ast.MakeAst(&ast.List{
			Elems: []*ast.Ast{
				ast.MakeAst(makeAtom(ast.AtomType_BOOL, "123")),
			},
		}),
	})

	// visiting table with non-list or non-table doesn't work
	testError(t, &ast.Table{Inner: ast.MakeAst(makeAtom(ast.AtomType_INT, "123"))})
	testError(t, &ast.Table{Inner: ast.MakeAst(makeAtom(ast.AtomType_STRING, "123"))})
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{}})})

	// even for lists, it only works when its items are dicts
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.List{Elems: []*ast.Ast{
		ast.MakeAst(makeAtom(ast.AtomType_INT, "123")),
	}})})
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.List{Elems: []*ast.Ast{
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{}}),
		ast.MakeAst(makeAtom(ast.AtomType_INT, "123")),
	}})})

	// and even then, it only works when they all have the same schema
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.List{Elems: []*ast.Ast{
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{
			"a": ast.MakeAst(makeAtom(ast.AtomType_INT, "123")),
			"b": ast.MakeAst(makeAtom(ast.AtomType_BOOL, "true")),
		}}),
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{}}),
	}})})
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.List{Elems: []*ast.Ast{
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{
			"a": ast.MakeAst(makeAtom(ast.AtomType_INT, "123")),
			"b": ast.MakeAst(makeAtom(ast.AtomType_BOOL, "true")),
		}}),
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{
			"a": ast.MakeAst(makeAtom(ast.AtomType_INT, "123")),
			"c": ast.MakeAst(makeAtom(ast.AtomType_BOOL, "true")),
		}}),
	}})})

	// same for nested
	testError(t, &ast.Table{Inner: ast.MakeAst(&ast.List{Elems: []*ast.Ast{
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{
			"a": ast.MakeAst(makeAtom(ast.AtomType_INT, "123")),
			"b": ast.MakeAst(&ast.Dict{
				Values: map[string]*ast.Ast{
					"inner": ast.MakeAst(makeAtom(ast.AtomType_INT, "123")),
				},
			}),
		}}),
		ast.MakeAst(&ast.Dict{Values: map[string]*ast.Ast{
			"a": ast.MakeAst(makeAtom(ast.AtomType_INT, "123")),
			"c": ast.MakeAst(makeAtom(ast.AtomType_BOOL, "true")),
		}}),
	}})})
}

func TestInterpreter_VisitOpcall(t *testing.T) {
	astrow1 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"a.inner": ast.MakeAst(makeAtom(ast.AtomType_INT, "3")),
			"b":       ast.MakeAst(makeAtom(ast.AtomType_STRING, "hi")),
		},
	}
	astrow2 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"a": ast.MakeAst(&ast.Dict{
				Values: map[string]*ast.Ast{
					"inner": ast.MakeAst(makeAtom(ast.AtomType_INT, "5")),
				}}),
			"b": ast.MakeAst(makeAtom(ast.AtomType_STRING, "bye")),
		},
	}
	astrow3 := &ast.Dict{
		Values: map[string]*ast.Ast{
			"b":       ast.MakeAst(makeAtom(ast.AtomType_STRING, "hello")),
			"a.inner": ast.MakeAst(makeAtom(ast.AtomType_INT, "3")),
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
			"where": ast.MakeAst(makeAtom(ast.AtomType_BOOL, "true")),
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
			"where": ast.MakeAst(makeAtom(ast.AtomType_BOOL, "false")),
		},
	}
	testValid(t, &ast.OpCall{
		Operand:   ast.MakeAst(astTable),
		Namespace: "std",
		Name:      "filter",
		Kwargs:    kwargs,
	}, value.NewTable())
}
