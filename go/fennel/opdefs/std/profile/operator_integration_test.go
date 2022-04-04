//go:build integration

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
	"fennel/test/optest"

	"github.com/stretchr/testify/assert"
)

func TestProfileOpMultipleObjs(t *testing.T) {
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
		Operands:  []ast.Ast{ast.Var{Name: "actions"}},
		Vars:      []string{"at"},
		Namespace: "std",
		Name:      "profile",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"otype": ast.Lookup{On: ast.Var{Name: "at"}, Property: "otype"},
			"oid":   ast.Lookup{On: ast.Var{Name: "at"}, Property: "oid"},
			"key":   ast.Lookup{On: ast.Var{Name: "at"}, Property: "key"},
			"field": ast.MakeString("profile_value"),
			// since version is an optional value, we don't pass it and still get the latest value back
		}},
	}
	i := interpreter.NewInterpreter(bootarg.Create(tier))
	table := value.List{}
	table.Append(value.NewDict(map[string]value.Value{"otype": value.String(otype1), "oid": value.Int(oid1), "key": value.String(key1), "ver": value.Int(ver1)}))
	table.Append(value.NewDict(map[string]value.Value{"otype": value.String(otype2), "oid": value.Int(oid2), "key": value.String(key2), "ver": value.Int(ver2)}))
	out, err := i.Eval(query, value.NewDict(map[string]value.Value{"actions": table}))
	assert.NoError(t, err)
	rows := out.(value.List)
	//assert.Len(t, rows, 2)
	assert.Equal(t, 2, rows.Len())
	r, _ := rows.At(0)
	assert.Equal(t, value.NewDict(map[string]value.Value{"otype": value.String(otype1), "oid": value.Int(oid1), "key": value.String(key1), "ver": value.Int(ver1), "profile_value": val1}), r)
	r, _ = rows.At(1)
	assert.Equal(t, value.NewDict(map[string]value.Value{"otype": value.String(otype2), "oid": value.Int(oid2), "key": value.String(key2), "ver": value.Int(ver2), "profile_value": val2}), r)
}

func TestNonDictProfile(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	// Set some profiles.
	ctx := context.Background()
	otype, key := ftypes.OType("user"), "age"
	req1a := profilelib.ProfileItem{OType: otype, Oid: 1, Key: key, Value: value.Int(13)}
	assert.NoError(t, profile.Set(ctx, tier, req1a))
	req1b := profilelib.ProfileItem{OType: otype, Oid: 2, Key: key, Value: value.Int(15)}
	assert.NoError(t, profile.Set(ctx, tier, req1b))

	intable := []value.Value{
		value.NewDict(map[string]value.Value{
			"index": value.Int(1),
		}),
		value.NewDict(map[string]value.Value{
			"index": value.Int(2),
		}),
		value.NewDict(map[string]value.Value{
			"index": value.Int(5),
		}),
	}
	staticKwargs := value.NewDict(map[string]value.Value{
		"default": value.Int(10),
	})
	contextKwargs := []value.Dict{
		value.NewDict(map[string]value.Value{
			"otype": value.String(otype),
			"key":   value.String(key),
			"oid":   value.Int(1),
		}),
		value.NewDict(map[string]value.Value{
			"otype": value.String(otype),
			"key":   value.String(key),
			"oid":   value.Int(2),
		}),
		value.NewDict(map[string]value.Value{
			"otype": value.String(otype),
			"key":   value.String(key),
			"oid":   value.Int(5),
		}),
	}

	optest.AssertElementsMatch(t, tier, &profileOp{tier: tier}, staticKwargs, intable, contextKwargs, []value.Value{
		value.Int(13),
		value.Int(15),
		value.Int(10),
	})
}
