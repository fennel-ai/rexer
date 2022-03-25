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
	i := interpreter.NewInterpreter(bootarg.Create(tier))
	table := value.List{}
	err = table.Append(value.NewDict(map[string]value.Value{}))
	assert.NoError(t, err)
	err = table.Append(value.NewDict(map[string]value.Value{}))
	assert.NoError(t, err)
	out, err := i.Eval(query, value.NewDict(map[string]value.Value{"actions": table}))
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

	// mini-redis does not work well cache keys being in different slots
	// we run a trimmed version of the test as unit test and add cases where the keys are stored in different
	// slots in `_integration_test`
	otype1, oid1, key1, val1, ver1 := ftypes.OType("user"), uint64(223), "age", value.Int(7), uint64(4)
	req1a := profilelib.ProfileItem{OType: otype1, Oid: oid1, Key: key1, Version: ver1 - 1, Value: value.Int(1121)}
	assert.NoError(t, profile.Set(ctx, tier, req1a))
	// this key has multiple versions but we should pick up the latest one if not provided explicitly
	req1b := profilelib.ProfileItem{OType: otype1, Oid: oid1, Key: key1, Version: ver1, Value: val1}
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
	i := interpreter.NewInterpreter(bootarg.Create(tier))
	table := value.NewList()
	err = table.Append(value.NewDict(map[string]value.Value{"otype": value.String(otype1), "oid": value.Int(oid1), "key": value.String(key1), "ver": value.Int(ver1)}))
	assert.NoError(t, err)
	out, err := i.Eval(query, value.NewDict(map[string]value.Value{"actions": table}))
	assert.NoError(t, err)
	rows := out.(value.List)
	//assert.Len(t, rows, 1)
	assert.Equal(t, 1, rows.Len())
	r, _ := rows.At(0)
	assert.Equal(t, value.NewDict(map[string]value.Value{"otype": value.String(otype1), "oid": value.Int(oid1), "key": value.String(key1), "ver": value.Int(ver1), "profile_value": val1}), r)
}
