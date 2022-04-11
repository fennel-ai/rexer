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
	i := interpreter.NewInterpreter(bootarg.Create(tier), map[string]interface{}{})
	table := value.List{}
	table.Append(value.NewDict(map[string]value.Value{"otype": value.String(otype1), "oid": value.Int(oid1), "key": value.String(key1), "ver": value.Int(ver1)}))
	table.Append(value.NewDict(map[string]value.Value{"otype": value.String(otype2), "oid": value.Int(oid2), "key": value.String(key2), "ver": value.Int(ver2)}))
	out, err := i.Eval(query, value.NewDict(map[string]value.Value{"actions": table}))
	assert.NoError(t, err)
	rows := out.(value.List)
	// assert.Len(t, rows, 2)
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

	optest.AssertElementsMatch(t, tier, &profileOp{tier: tier}, staticKwargs, [][]value.Value{intable}, contextKwargs, []value.Value{
		value.Int(13),
		value.Int(15),
		value.Int(10),
	})
}

func TestProfileOpCacheMultiple(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	query := ast.OpCall{
		Operands:  []ast.Ast{ast.Var{Name: "actions"}},
		Vars:      []string{"a"},
		Namespace: "std",
		Name:      "profile",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"otype":   ast.Lookup{On: ast.Var{Name: "a"}, Property: "otype"},
			"oid":     ast.Lookup{On: ast.Var{Name: "a"}, Property: "oid"},
			"key":     ast.Lookup{On: ast.Var{Name: "a"}, Property: "key"},
			"version": ast.Lookup{On: ast.Var{Name: "a"}, Property: "version"},
			"field":   ast.MakeString("profile_value"),
		}},
	}
	profiles := []profilelib.ProfileItem{
		{"1", 2, "3", 4, value.Int(5)},
		{"6", 7, "8", 9, value.Int(10)},
		{"11", 12, "13", 14, value.Int(15)},
	}
	inTable := value.NewList()
	for _, pi := range profiles {
		inTable.Append(value.NewDict(map[string]value.Value{
			"otype":   value.String(pi.OType),
			"oid":     value.Int(pi.Oid),
			"key":     value.String(pi.Key),
			"version": value.Int(pi.Version),
		}))
	}
	// to query for version = 0
	inTable0 := value.NewList()
	for _, pi := range profiles {
		inTable0.Append(value.NewDict(map[string]value.Value{
			"otype":   value.String(pi.OType),
			"oid":     value.Int(pi.Oid),
			"key":     value.String(pi.Key),
			"version": value.Int(0),
		}))
	}

	i := interpreter.NewInterpreter(bootarg.Create(tier), map[string]interface{}{})
	var expected []value.Dict
	for _, pi := range profiles {
		expected = append(expected, value.NewDict(map[string]value.Value{
			"otype":         value.String(pi.OType),
			"oid":           value.Int(pi.Oid),
			"key":           value.String(pi.Key),
			"version":       value.Int(pi.Version),
			"profile_value": value.Nil,
		}))
	}
	verifyMultiple(t, &i, query, inTable, expected)

	// test cache by setting profiles now
	for _, pi := range profiles {
		assert.NoError(t, profile.Set(ctx, tier, pi))
	}
	// we should still get back default value if it is cached properly
	verifyMultiple(t, &i, query, inTable, expected)

	// now use a new interpreter with fresh cache, should get back stored value now
	i = interpreter.NewInterpreter(bootarg.Create(tier), map[string]interface{}{})
	var expected2 []value.Dict
	for _, pi := range profiles {
		expected2 = append(expected2, value.NewDict(map[string]value.Value{
			"otype":         value.String(pi.OType),
			"oid":           value.Int(pi.Oid),
			"key":           value.String(pi.Key),
			"version":       value.Int(pi.Version),
			"profile_value": pi.Value,
		}))
	}
	verifyMultiple(t, &i, query, inTable, expected2)

	// now store a newer version with new values for each profile
	for _, pi := range profiles {
		pi2 := pi
		pi2.Version++
		pi2.Value, err = pi2.Value.Op("+", value.Int(2))
		assert.NoError(t, err)
		assert.NoError(t, profile.Set(ctx, tier, pi2))
	}
	// now if we use version = 0, we should get back latest profile even though older version is cached
	var expected3 []value.Dict
	for _, pi := range profiles {
		newval, err := pi.Value.Op("+", value.Int(2))
		assert.NoError(t, err)
		expected3 = append(expected3, value.NewDict(map[string]value.Value{
			"otype":         value.String(pi.OType),
			"oid":           value.Int(pi.Oid),
			"key":           value.String(pi.Key),
			"version":       value.Int(0),
			"profile_value": newval,
		}))
	}
	verifyMultiple(t, &i, query, inTable0, expected3)

	// but once version = 0 is cached, we should not get any later versions
	for _, pi := range profiles {
		pi3 := pi
		pi3.Version += 2
		pi3.Value, err = pi3.Value.Op("+", value.Int(5))
		assert.NoError(t, err)
		assert.NoError(t, profile.Set(ctx, tier, pi3))
	}
	verifyMultiple(t, &i, query, inTable0, expected3)
}

func verifyMultiple(t *testing.T, i *interpreter.Interpreter, query ast.Ast, table value.List, expected []value.Dict) {
	out, err := i.Eval(query, value.NewDict(map[string]value.Value{"actions": table}))
	assert.NoError(t, err)
	rows := out.(value.List)
	assert.Equal(t, len(expected), rows.Len())
	for i, exp := range expected {
		found, _ := rows.At(i)
		assert.True(t, exp.Equal(found))
	}
}
