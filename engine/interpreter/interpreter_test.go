package interpreter

import (
	"engine/ast"
	"engine/runtime"
	"github.com/stretchr/testify/assert"
	"testing"
)

func getInterpreter() Interpreter {
	return Interpreter{
		runtime.NewEnv(),
	}
}

func testValid(t *testing.T, node ast.Ast, expected runtime.Value) {
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
	testValid(t, ast.Atom{Type: ast.Int, Lexeme: "123"}, runtime.Int(123))
	testValid(t, ast.Atom{Type: ast.Int, Lexeme: "-123"}, runtime.Int(-123))
	testValid(t, ast.Atom{Type: ast.Double, Lexeme: "123"}, runtime.Double(123.0))
	testValid(t, ast.Atom{Type: ast.Double, Lexeme: "123.3"}, runtime.Double(123.3))
	testValid(t, ast.Atom{Type: ast.Double, Lexeme: "-123.3"}, runtime.Double(-123.3))
	// possible to parse double without decimal
	testValid(t, ast.Atom{Type: ast.Double, Lexeme: "123"}, runtime.Double(123.0))
	testValid(t, ast.Atom{Type: ast.Bool, Lexeme: "true"}, runtime.Bool(true))
	testValid(t, ast.Atom{Type: ast.Bool, Lexeme: "false"}, runtime.Bool(false))
	// possible to parse bools from ints
	testValid(t, ast.Atom{Type: ast.Bool, Lexeme: "1"}, runtime.Bool(true))
	testValid(t, ast.Atom{Type: ast.Bool, Lexeme: "0"}, runtime.Bool(false))

	testValid(t, ast.Atom{Type: ast.String, Lexeme: "hi"}, runtime.String("hi"))
	testValid(t, ast.Atom{Type: ast.String, Lexeme: "false"}, runtime.String("false"))
	testValid(t, ast.Atom{Type: ast.String, Lexeme: "3.2"}, runtime.String("3.2"))

	// invalid checks
	testError(t, ast.Atom{Type: ast.Int, Lexeme: "true"})
	testError(t, ast.Atom{Type: ast.Int, Lexeme: "123.0"})
	testError(t, ast.Atom{Type: ast.Int, Lexeme: "hi"})

	testError(t, ast.Atom{Type: ast.Double, Lexeme: "true"})
	testError(t, ast.Atom{Type: ast.Double, Lexeme: "hi"})

	testError(t, ast.Atom{Type: ast.Bool, Lexeme: "5"})
	testError(t, ast.Atom{Type: ast.Bool, Lexeme: "3.2"})
	testError(t, ast.Atom{Type: ast.Bool, Lexeme: "hi"})

}

func TestInterpreter_VisitBinary(t *testing.T) {
	testValid(t, ast.Binary{
		Left:  ast.Atom{Type: ast.Int, Lexeme: "5"},
		Op:    "+",
		Right: ast.Atom{Type: ast.Int, Lexeme: "8"},
	}, runtime.Int(13))

	// and errors are propagated too - both parse errors...
	testError(t, ast.Binary{
		Left:  ast.Atom{Type: ast.Int, Lexeme: "5"},
		Op:    "*",
		Right: ast.Atom{Type: ast.Int, Lexeme: "false"},
	})
	// ...and type errors
	testError(t, ast.Binary{
		Left:  ast.Atom{Type: ast.Int, Lexeme: "5"},
		Op:    "*",
		Right: ast.Atom{Type: ast.Bool, Lexeme: "false"},
	})
}

func TestInterpreter_VisitList(t *testing.T) {
	// Empty list works
	testValid(t, ast.List{[]ast.Ast{}}, runtime.List{})

	// list with just one element works
	l, _ := runtime.NewList([]runtime.Value{runtime.Double(3.4)})
	testValid(t, ast.List{[]ast.Ast{ast.Atom{
		ast.Double, "3.4",
	}}}, l)
	// and so does a multi-element list with mixed types
	l, _ = runtime.NewList([]runtime.Value{runtime.Double(3.4), runtime.Bool(false), runtime.String("hi")})
	testValid(t, ast.List{[]ast.Ast{
		ast.Atom{ast.Double, "3.4"},
		ast.Atom{ast.Bool, "false"},
		ast.Atom{ast.String, "hi"},
	}}, l)
}

func TestInterpreter_VisitDict(t *testing.T) {
	// Empty dict works
	testValid(t, ast.Dict{map[string]ast.Ast{}}, runtime.Dict{})

	// dict with just one element works
	d, _ := runtime.NewDict(map[string]runtime.Value{"hi": runtime.Double(3.4)})
	testValid(t, ast.Dict{map[string]ast.Ast{
		"hi": ast.Atom{ast.Double, "3.4"},
	}}, d)
	// and so does a multi-element list with mixed types and nesting
	nested, _ := runtime.NewDict(map[string]runtime.Value{
		"hi":     runtime.Double(3.4),
		"bye":    runtime.Bool(false),
		"nested": d,
	})
	testValid(t, ast.Dict{map[string]ast.Ast{
		"hi":  ast.Atom{ast.Double, "3.4"},
		"bye": ast.Atom{ast.Bool, "false"},
		"nested": ast.Dict{map[string]ast.Ast{
			"hi": ast.Atom{ast.Double, "3.4"},
		}},
	}}, nested)
}

func TestInterpreter_VisitStatement(t *testing.T) {
	s := ast.Statement{"var", ast.Atom{ast.Bool, "false"}}
	testValid(t, s, runtime.Bool(false))

	// same happens if no name is passed
	s = ast.Statement{"", ast.Atom{ast.Bool, "false"}}
	testValid(t, s, runtime.Bool(false))
}
