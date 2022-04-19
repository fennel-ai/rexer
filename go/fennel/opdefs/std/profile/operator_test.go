package profile

import (
	"context"
	"testing"

	"fennel/controller/profile"
	"fennel/engine/ast"
	"fennel/engine/interpreter"
	"fennel/engine/interpreter/bootarg"
	"fennel/lib/ftypes"
	profilelib "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestDefault(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	query := ast.OpCall{
		Operands:  []ast.Ast{ast.Var{Name: "actions"}},
		Namespace: "std",
		Name:      "profile",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"otype":   ast.MakeString("user"),
			"oid":     ast.MakeInt(123),
			"key":     ast.MakeString("some key"),
			"field":   ast.MakeString("some name"),
			"default": ast.MakeDouble(3.4),
		}},
	}
	table := value.List{}
	table.Append(value.NewDict(map[string]value.Value{}))
	table.Append(value.NewDict(map[string]value.Value{}))
	i, err := interpreter.NewInterpreter(context.Background(), bootarg.Create(tier),
		value.NewDict(map[string]value.Value{"actions": table}))
	assert.NoError(t, err)
	out, err := query.AcceptValue(i)
	assert.NoError(t, err)
	rows := out.(value.List)
	//assert.Len(t, rows, 2)
	assert.Equal(t, 2, rows.Len())
	r, _ := rows.At(0)
	assert.Equal(t, value.NewDict(map[string]value.Value{"some name": value.Double(3.4)}), r)
	r, _ = rows.At(1)
	assert.Equal(t, value.NewDict(map[string]value.Value{"some name": value.Double(3.4)}), r)
}

func TestProfileOp(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	// mini-redis does not work well with cache keys being in different slots
	// we run a trimmed version of the test with different versions of the same profile as unit test
	// and add cases where the keys are stored in different slots in `_integration_test`
	otype1, oid1, key1, val1, ver1 := ftypes.OType("user"), uint64(223), "age", value.Int(7), uint64(4)
	req1a := profilelib.ProfileItem{OType: otype1, Oid: oid1, Key: key1, UpdateTime: ver1 - 1, Value: value.Int(1121)}
	assert.NoError(t, profile.Set(ctx, tier, req1a))
	// this key has multiple versions but we should pick up the latest one if not provided explicitly
	req1b := profilelib.ProfileItem{OType: otype1, Oid: oid1, Key: key1, UpdateTime: ver1, Value: val1}
	assert.NoError(t, profile.Set(ctx, tier, req1b))

	query := ast.OpCall{
		Operands:  []ast.Ast{ast.Var{Name: "actions"}},
		Vars:      []string{"a"},
		Namespace: "std",
		Name:      "profile",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"otype": ast.Lookup{On: ast.Var{Name: "a"}, Property: "otype"},
			"oid":   ast.Lookup{On: ast.Var{Name: "a"}, Property: "oid"},
			"key":   ast.Lookup{On: ast.Var{Name: "a"}, Property: "key"},
			"field": ast.MakeString("profile_value"),
			// since version is an optional value, we don't pass it and still get the latest value back
		}},
	}
	table := value.NewList()
	table.Append(value.NewDict(map[string]value.Value{
		"otype": value.String(otype1),
		"oid":   value.Int(oid1),
		"key":   value.String(key1),
	}))
	i, err := interpreter.NewInterpreter(context.Background(), bootarg.Create(tier),
		value.NewDict(map[string]value.Value{"actions": table}))
	assert.NoError(t, err)
	expected := value.NewDict(map[string]value.Value{
		"otype":         value.String(otype1),
		"oid":           value.Int(oid1),
		"key":           value.String(key1),
		"profile_value": val1,
	})
	verify(t, &i, query, expected)
}

func TestProfileOpCache(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	otype, oid, key, val, ver := ftypes.OType("user"), uint64(223), "age", value.Int(7), uint64(4)
	query := ast.OpCall{
		Operands:  []ast.Ast{ast.Var{Name: "actions"}},
		Vars:      []string{"a"},
		Namespace: "std",
		Name:      "profile",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"otype": ast.Lookup{On: ast.Var{Name: "a"}, Property: "otype"},
			"oid":   ast.Lookup{On: ast.Var{Name: "a"}, Property: "oid"},
			"key":   ast.Lookup{On: ast.Var{Name: "a"}, Property: "key"},
			"field": ast.MakeString("profile_value"),
		}},
	}

	inTable := value.NewList()
	inTable.Append(value.NewDict(map[string]value.Value{
		"otype": value.String(otype),
		"oid":   value.Int(oid),
		"key":   value.String(key),
	}))
	expected := value.NewDict(map[string]value.Value{
		"otype":         value.String(otype),
		"oid":           value.Int(oid),
		"key":           value.String(key),
		"profile_value": value.Nil,
	})
	i, err := interpreter.NewInterpreter(context.Background(), bootarg.Create(tier),
		value.NewDict(map[string]value.Value{"actions": inTable}))
	assert.NoError(t, err)
	verify(t, &i, query, expected)

	// test cache by setting a profile now
	req1 := profilelib.ProfileItem{OType: otype, Oid: oid, Key: key, UpdateTime: ver, Value: val}
	assert.NoError(t, profile.Set(ctx, tier, req1))
	// we should still get back default value if it is cached properly
	verify(t, &i, query, expected)

	// use a new interpreter now, should get back stored value now
	i, err = interpreter.NewInterpreter(context.Background(), bootarg.Create(tier),
		value.NewDict(map[string]value.Value{"actions": inTable}))
	assert.NoError(t, err)
	expected2 := expected.Clone().(value.Dict)
	expected2.Set("profile_value", val)
	verify(t, &i, query, expected2)
}

func verify(t *testing.T, i *interpreter.Interpreter, query ast.Ast, expected value.Dict) {
	out, err := query.AcceptValue(i)
	assert.NoError(t, err)
	rows := out.(value.List)
	assert.Equal(t, 1, rows.Len())
	found, _ := rows.At(0)
	assert.True(t, expected.Equal(found))
}
