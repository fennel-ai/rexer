package interpreter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/engine/ast"
	"fennel/lib/value"
)

func TestInterpreter_VisitHighFnCall_Map(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		tree     ast.Ast
		err      bool
		expected value.Value
	}{
		{
			tree: ast.HighFnCall{
				Type:    ast.Map,
				Varname: "x",
				Lambda:  ast.Binary{Left: ast.Var{Name: "x"}, Op: "*", Right: ast.MakeInt(2)},
				Iter:    ast.List{Values: []ast.Ast{ast.MakeInt(1), ast.MakeInt(2), ast.MakeInt(-1)}},
			},
			err:      false,
			expected: value.List{value.Int(2), value.Int(4), value.Int(-2)},
		},
		{
			tree: ast.HighFnCall{
				Type:    ast.Map,
				Varname: "with space",
				Lambda:  ast.Binary{Left: ast.Var{Name: "with space"}, Op: "*", Right: ast.Lookup{On: ast.Var{"args"}, Property: "n"}},
				Iter:    ast.List{Values: []ast.Ast{ast.MakeInt(1), ast.MakeInt(2), ast.MakeInt(-1)}},
			},
			err:      false,
			expected: value.List{value.Int(3), value.Int(6), value.Int(-3)},
		},
		{
			tree: ast.HighFnCall{
				Type:    ast.Map,
				Varname: "x",
				Lambda:  ast.Binary{Left: ast.Var{Name: "y"}, Op: "*", Right: ast.MakeInt(1)},
				Iter:    ast.List{Values: []ast.Ast{ast.MakeInt(1), ast.MakeInt(2), ast.MakeInt(-1)}},
			},
			err:      true,
			expected: nil,
		},
		{
			tree: ast.HighFnCall{
				Type:    ast.Map,
				Varname: "x",
				Lambda:  ast.Binary{Left: ast.Var{Name: "x"}, Op: "*", Right: ast.MakeBool(false)},
				Iter:    ast.List{Values: []ast.Ast{ast.MakeInt(1), ast.MakeInt(2), ast.MakeInt(-1)}},
			},
			err:      true,
			expected: nil,
		},
		{
			// case where we "shadow" variable and read iter from another variable
			tree: ast.Query{
				Statements: []ast.Statement{
					{Name: "x", Body: ast.MakeInt(1)},
					{Name: "l", Body: ast.List{Values: []ast.Ast{ast.MakeInt(2), ast.MakeInt(4), ast.MakeInt(3)}}},
					{Name: "ret", Body: ast.HighFnCall{
						Type:    ast.Map,
						Varname: "x",
						Lambda:  ast.Binary{Left: ast.Var{Name: "x"}, Op: ">=", Right: ast.Lookup{On: ast.Var{"args"}, Property: "n"}},
						Iter:    ast.Var{"l"},
					}},
				},
			},
			err:      false,
			expected: value.List{value.Bool(false), value.Bool(true), value.Bool(true)},
		},
	}
	for _, scene := range scenarios {
		i := getInterpreter()
		res, err := i.Eval(scene.tree, value.Dict{"n": value.Int(3)})
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.expected, res)
		}
	}
}

func TestInterpreter_VisitHighFnCall_Filter(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		tree     ast.Ast
		err      bool
		expected value.Value
	}{
		{
			tree: ast.HighFnCall{
				Type:    ast.Filter,
				Varname: "x",
				Lambda:  ast.Binary{Left: ast.Var{Name: "x"}, Op: ">=", Right: ast.Lookup{On: ast.Var{"args"}, Property: "n"}},
				Iter:    ast.List{Values: []ast.Ast{ast.MakeInt(2), ast.MakeInt(4), ast.MakeInt(3)}},
			},
			err:      false,
			expected: value.List{value.Int(4), value.Int(3)},
		},
		{
			tree: ast.HighFnCall{
				Type:    ast.Filter,
				Varname: "x",
				Lambda:  ast.Binary{Left: ast.Var{Name: "y"}, Op: ">=", Right: ast.Lookup{On: ast.Var{"args"}, Property: "n"}},
				Iter:    ast.List{Values: []ast.Ast{ast.MakeInt(2), ast.MakeInt(4), ast.MakeInt(3)}},
			},
			err:      true,
			expected: nil,
		},
		{
			tree: ast.HighFnCall{
				Type:    ast.Filter,
				Varname: "x",
				Lambda:  ast.Binary{Left: ast.Var{Name: "x"}, Op: ">=", Right: ast.MakeBool(false)},
				Iter:    ast.List{Values: []ast.Ast{ast.MakeInt(2), ast.MakeInt(4), ast.MakeInt(3)}},
			},
			err:      true,
			expected: nil,
		},
	}
	for _, scene := range scenarios {
		i := getInterpreter()
		res, err := i.Eval(scene.tree, value.Dict{"n": value.Int(3)})
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.expected, res)
		}
	}
}
