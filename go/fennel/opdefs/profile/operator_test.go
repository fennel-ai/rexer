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
		Operand: ast.Lookup{
			On:       ast.Var{"args"},
			Property: "actions",
		},
		Namespace: "profile",
		Name:      "addField",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"otype":   ast.MakeString("user"),
			"oid":     ast.MakeInt(123),
			"key":     ast.MakeString("some key"),
			"name":    ast.MakeString("some name"),
			"default": ast.MakeDouble(3.4),
		}},
	}
	i := interpreter.NewInterpreter(bootarg.Create(tier))
	table := value.List{}
	err = table.Append(value.Dict{})
	assert.NoError(t, err)
	err = table.Append(value.Dict{})
	assert.NoError(t, err)
	out, err := i.Eval(query, value.Dict{"actions": table})
	assert.NoError(t, err)
	rows := out.(value.List)
	assert.Len(t, rows, 2)
	assert.Equal(t, value.Dict{"some name": value.Double(3.4)}, rows[0])
	assert.Equal(t, value.Dict{"some name": value.Double(3.4)}, rows[1])
}

func TestProfileOp(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	otype1, oid1, key1, val1, ver1 := ftypes.OType("user"), uint64(123), "summary", value.Int(5), uint64(1)
	otype2, oid2, key2, val2, ver2 := ftypes.OType("user"), uint64(223), "age", value.Int(7), uint64(4)
	req1 := profilelib.ProfileItem{OType: otype1, Oid: oid1, Key: key1, Version: ver1, Value: val1}
	assert.NoError(t, profile.Set(ctx, tier, req1))
	req2a := profilelib.ProfileItem{OType: otype2, Oid: oid2, Key: key2, Version: ver2 - 1, Value: value.Int(1121)}
	assert.NoError(t, profile.Set(ctx, tier, req2a))
	// this key has multiple versions but we should pick up the latest one if not provided explicitly
	req2b := profilelib.ProfileItem{OType: otype2, Oid: oid2, Key: key2, Version: ver2, Value: val2}
	assert.NoError(t, profile.Set(ctx, tier, req2b))

	query := ast.OpCall{
		Operand: ast.Lookup{
			On:       ast.Var{"args"},
			Property: "actions",
		},
		Namespace: "profile",
		Name:      "addField",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"otype": ast.Lookup{On: ast.At{}, Property: "otype"},
			"oid":   ast.Lookup{On: ast.At{}, Property: "oid"},
			"key":   ast.Lookup{On: ast.At{}, Property: "key"},
			"name":  ast.MakeString("profile_value"),
			// since version is an optional value, we don't pass it and still get the latest value back
		}},
	}
	i := interpreter.NewInterpreter(bootarg.Create(tier))
	table := value.List{}
	err = table.Append(value.Dict{"otype": value.String(otype1), "oid": value.Int(oid1), "key": value.String(key1), "ver": value.Int(ver1)})
	assert.NoError(t, err)
	err = table.Append(value.Dict{"otype": value.String(otype2), "oid": value.Int(oid2), "key": value.String(key2), "ver": value.Int(ver2)})
	assert.NoError(t, err)
	out, err := i.Eval(query, value.Dict{"actions": table})
	assert.NoError(t, err)
	rows := out.(value.List)
	assert.Len(t, rows, 2)
	assert.Equal(t, value.Dict{"otype": value.String(otype1), "oid": value.Int(oid1), "key": value.String(key1), "ver": value.Int(ver1), "profile_value": val1}, rows[0])
	assert.Equal(t, value.Dict{"otype": value.String(otype2), "oid": value.Int(oid2), "key": value.String(key2), "ver": value.Int(ver2), "profile_value": val2}, rows[1])
}
