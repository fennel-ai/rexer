package interpreter

import (
	"fennel/engine/ast"
	"fennel/lib/value"
	"fmt"
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

func makeInt(i int32) ast.Atom {
	return ast.Atom{Type: ast.Int, Lexeme: fmt.Sprintf("%d", i)}
}

func makeDouble(d float64) ast.Atom {
	return ast.Atom{Type: ast.Double, Lexeme: fmt.Sprintf("%f", d)}
}

func makeString(s string) ast.Atom {
	return ast.Atom{Type: ast.String, Lexeme: s}
}

func makeBool(b bool) ast.Atom {
	var str string
	if b {
		str = "true"
	} else {
		str = "false"
	}
	return ast.Atom{Type: ast.Bool, Lexeme: str}
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
	testValid(t, ast.Binary{
		Left:  makeInt(5),
		Op:    "+",
		Right: makeInt(8),
	}, value.Int(13))

	// and errors are propagated through type errors.
	testError(t, ast.Binary{
		Left:  makeInt(5),
		Op:    "*",
		Right: makeBool(false),
	})
}

func TestInterpreter_VisitList(t *testing.T) {
	// Empty list works
	testValid(t, ast.List{Values: []ast.Ast{}}, value.List{})

	// list with just one element works
	l, _ := value.NewList([]value.Value{value.Double(3.4)})
	testValid(t, &ast.List{Values: []ast.Ast{makeDouble(3.4)}}, l)
	// and so does a multi-element list with mixed types
	l, _ = value.NewList([]value.Value{value.Double(3.4), value.Bool(false), value.String("hi")})
	testValid(t, &ast.List{Values: []ast.Ast{makeDouble(3.4), makeBool(false), makeString("hi")}}, l)
}

func TestInterpreter_VisitDict(t *testing.T) {
	// Empty dict works
	testValid(t, ast.Dict{Values: map[string]ast.Ast{}}, value.Dict{})

	// dict with just one element works
	d, _ := value.NewDict(map[string]value.Value{"hi": value.Double(3.4)})
	testValid(t, ast.Dict{Values: map[string]ast.Ast{"hi": makeDouble(3.4)}}, d)
	// and so does a multi-element list with mixed types and nesting
	nested, _ := value.NewDict(map[string]value.Value{
		"hi":     value.Double(3.4),
		"bye":    value.Bool(false),
		"nested": d,
	})
	testValid(t, ast.Dict{Values: map[string]ast.Ast{
		"hi":  makeDouble(3.4),
		"bye": makeBool(false),
		"nested": ast.Dict{
			Values: map[string]ast.Ast{
				"hi": makeDouble(3.4),
			}}}}, nested)
}

func TestInterpreter_VisitStatement(t *testing.T) {
	s := ast.Statement{Name: "var", Body: makeBool(false)}
	testValid(t, s, value.Bool(false))

	// same happens if no name is passed
	s = ast.Statement{Name: "", Body: makeBool(false)}
	testValid(t, s, value.Bool(false))
}

func TestInterpreter_VisitTable(t *testing.T) {
	astrow1 := ast.Dict{
		Values: map[string]ast.Ast{
			"a.inner": makeInt(3),
			"b":       makeString("hi"),
		},
	}
	astrow2 := ast.Dict{
		Values: map[string]ast.Ast{
			"a": ast.Dict{
				Values: map[string]ast.Ast{
					"inner": makeInt(5),
				}},
			"b": makeString("bye"),
		},
	}
	astrow3 := ast.Dict{
		Values: map[string]ast.Ast{
			"b":       makeString("hello"),
			"a.inner": makeInt(3),
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
	testError(t, ast.Table{Inner: makeInt(123)})
	testError(t, ast.Table{Inner: makeString("123")})
	testError(t, ast.Table{Inner: ast.Dict{Values: map[string]ast.Ast{}}})

	// even for lists, it only works when its items are dicts
	testError(t, ast.Table{Inner: ast.List{Values: []ast.Ast{makeInt(123)}}})
	testError(t, ast.Table{Inner: ast.List{Values: []ast.Ast{ast.Dict{Values: map[string]ast.Ast{}}, makeInt(123)}}})

	// and even then, it only works when they all have the same schema
	testError(t, &ast.Table{Inner: ast.List{Values: []ast.Ast{ast.Dict{Values: map[string]ast.Ast{
		"a": makeInt(123),
		"b": makeBool(true),
	}},
		ast.Dict{Values: map[string]ast.Ast{}}}}},
	)

	testError(t, ast.Table{Inner: ast.List{Values: []ast.Ast{
		ast.Dict{Values: map[string]ast.Ast{
			"a": makeInt(123),
			"b": makeBool(true),
		}},
		ast.Dict{Values: map[string]ast.Ast{
			"a": makeInt(123),
			"c": makeBool(true),
		}},
	}}})

	// same for nested
	testError(t, &ast.Table{Inner: ast.List{Values: []ast.Ast{
		ast.Dict{Values: map[string]ast.Ast{
			"a": makeInt(123),
			"b": ast.Dict{
				Values: map[string]ast.Ast{
					"inner": makeInt(123),
				},
			},
		}},
		ast.Dict{Values: map[string]ast.Ast{
			"a": makeInt(123),
			"c": makeBool(false),
		}},
	}}})
}

func TestInterpreter_VisitOpcall(t *testing.T) {
	astrow1 := ast.Dict{
		Values: map[string]ast.Ast{
			"a.inner": makeInt(3),
			"b":       makeString("hi"),
		},
	}
	astrow2 := ast.Dict{
		Values: map[string]ast.Ast{
			"a": ast.Dict{
				Values: map[string]ast.Ast{
					"inner": makeInt(5),
				}},
			"b": makeString("bye"),
		},
	}
	astrow3 := ast.Dict{
		Values: map[string]ast.Ast{
			"b":       makeString("hello"),
			"a.inner": makeInt(3),
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

	kwargs := ast.Dict{
		Values: map[string]ast.Ast{
			"where": makeBool(true),
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
			"where": makeBool(false),
		},
	}
	testValid(t, ast.OpCall{
		Operand:   astTable,
		Namespace: "std",
		Name:      "filter",
		Kwargs:    kwargs,
	}, value.NewTable())
}
