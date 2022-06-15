package test

import (
	"fennel/engine/ast"
	_ "fennel/opdefs/std/map"
)

// Returns an aggregate query that simply extracts the "groupkey", "value",
// and "timestamp" fields from the action metadata.
func GetDummyAggQuery() ast.Ast {
	return &ast.OpCall{
		Namespace: "std",
		Name:      "map",
		Operands:  []ast.Ast{ast.MakeVar("actions")},
		Vars:      []string{"e"},
		Kwargs: ast.MakeDict(map[string]ast.Ast{
			"to": ast.MakeDict(map[string]ast.Ast{
				"groupkey":  ast.MakeLookup(ast.MakeLookup(ast.MakeVar("e"), "metadata"), "groupkey"),
				"value":     ast.MakeLookup(ast.MakeLookup(ast.MakeVar("e"), "metadata"), "value"),
				"timestamp": ast.MakeLookup(ast.MakeLookup(ast.MakeVar("e"), "metadata"), "timestamp"),
			}),
		}),
	}
}
