package profile

import (
	"fennel/engine/ast"
	"fennel/engine/interpreter"
	"fennel/engine/interpreter/bootarg"
	"fennel/lib/ftypes"
	profilelib "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProfileOp(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	otype1, oid1, key1, val1, ver1 := ftypes.OType("user"), uint64(123), "summary", value.Int(5), uint64(1)
	otype2, oid2, key2, val2, ver2 := ftypes.OType("user"), uint64(223), "age", value.Int(7), uint64(4)
	req1 := profilelib.ProfileItem{CustID: tier.CustID, OType: otype1, Oid: oid1, Key: key1, Version: ver1, Value: val1}
	assert.NoError(t, Set(tier, req1))
	req2a := profilelib.ProfileItem{CustID: tier.CustID, OType: otype2, Oid: oid2, Key: key2, Version: ver2 - 1, Value: value.Int(1121)}
	assert.NoError(t, Set(tier, req2a))
	// this key has multiple versions but we should pick up the latest one if not provided explicitly
	req2b := profilelib.ProfileItem{CustID: tier.CustID, OType: otype2, Oid: oid2, Key: key2, Version: ver2, Value: val2}
	assert.NoError(t, Set(tier, req2b))

	query := ast.OpCall{
		Operand:   ast.Var{Name: "table"},
		Namespace: "std",
		Name:      "addProfileColumn",
		Kwargs: ast.Dict{Values: map[string]ast.Ast{
			"otype": ast.Lookup{On: ast.At{}, Property: "otype"},
			"oid":   ast.Lookup{On: ast.At{}, Property: "oid"},
			"key":   ast.Lookup{On: ast.At{}, Property: "key"},
			// since version is an optional value, we don't pass it and still get the latest value back
		}},
	}
	i := interpreter.NewInterpreter(bootarg.Create(tier))
	table := value.NewTable()
	err = table.Append(value.Dict{"otype": value.String(otype1), "oid": value.Int(oid1), "key": value.String(key1), "ver": value.Int(ver1)})
	assert.NoError(t, err)
	err = table.Append(value.Dict{"otype": value.String(otype2), "oid": value.Int(oid2), "key": value.String(key2), "ver": value.Int(ver2)})
	assert.NoError(t, err)
	assert.NoError(t, i.SetVar("table", table))
	out, err := query.AcceptValue(i)
	assert.NoError(t, err)
	outtable := out.(value.Table)
	rows := outtable.Pull()
	assert.Len(t, rows, 2)
	assert.Equal(t, value.Dict{"otype": value.String(otype1), "oid": value.Int(oid1), "key": value.String(key1), "ver": value.Int(ver1), "profile_value": val1}, rows[0])
	assert.Equal(t, value.Dict{"otype": value.String(otype2), "oid": value.Int(oid2), "key": value.String(key2), "ver": value.Int(ver2), "profile_value": val2}, rows[1])
}
