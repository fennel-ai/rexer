package engine

import (
	"context"
	"testing"

	"fennel/engine/ast"
	"fennel/engine/interpreter/bootarg"
	"fennel/lib/value"
	"fennel/tier"

	"github.com/stretchr/testify/assert"
)

func TestQueryExecution(t *testing.T) {
	tr := tier.Tier{
		ID: 0,
	}
	executor := NewQueryExecutor(bootarg.Create(tr))
	// first, test some query
	query1 := ast.IfElse{
		Condition: ast.MakeBool(false),
		ThenDo:    ast.MakeInt(+1),
		ElseDo:    ast.MakeInt(-1),
	}
	args1 := value.NewDict(map[string]value.Value{})
	expected1 := value.Int(-1)

	found1, err := executor.Exec(context.Background(), query1, args1)
	assert.NoError(t, err)
	assert.True(t, expected1.Equal(found1))

	// now test a query which uses args
	query2 := ast.IfElse{
		Condition: ast.Var{Name: "x"},
		ThenDo:    ast.MakeInt(+1),
		ElseDo:    ast.MakeInt(-1),
	}
	args2 := value.NewDict(map[string]value.Value{"x": value.Bool(true)})
	expected2 := value.Int(+1)

	found2, err := executor.Exec(context.Background(), query2, args2)
	assert.NoError(t, err)
	assert.True(t, expected2.Equal(found2))

	// now test shadowing args
	query3 := ast.Query{Statements: []ast.Statement{
		{
			Name: "x",
			Body: ast.MakeBool(false),
		},
		{
			Name: "",
			Body: ast.IfElse{
				Condition: ast.Var{Name: "x"},
				ThenDo:    ast.MakeInt(+1),
				ElseDo:    ast.MakeInt(-1),
			},
		},
	}}
	args3 := value.NewDict(map[string]value.Value{"x": value.Bool(true)})
	expected3 := value.Int(-1)

	found3, err := executor.Exec(context.Background(), query3, args3)
	assert.NoError(t, err)
	assert.True(t, expected3.Equal(found3))
}
