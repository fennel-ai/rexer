package profile

import (
	"context"

	"fennel/controller/profile"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	"fennel/lib/ftypes"
	libprofile "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/tier"
)

func init() {
	operators.Register(profileOp{})
}

type profileOp struct {
	tier tier.Tier
}

func (p profileOp) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	tr, err := bootarg.GetTier(bootargs)
	if err != nil {
		return nil, err
	}
	return profileOp{tr}, nil
}

func (p profileOp) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	colname := string(staticKwargs.GetUnsafe("name").(value.String))
	reqs := make([]libprofile.ProfileItem, 0)
	rows := make([]value.Dict, 0)
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		rowVal := heads[0]
		row := rowVal.(value.Dict)
		req := libprofile.ProfileItem{
			OType:   ftypes.OType(kwargs.GetUnsafe("otype").(value.String)),
			Oid:     uint64(kwargs.GetUnsafe("oid").(value.Int)),
			Key:     string(kwargs.GetUnsafe("key").(value.String)),
			Version: uint64(kwargs.GetUnsafe("version").(value.Int)),
		}
		reqs = append(reqs, req)
		rows = append(rows, row)
	}
	vals, err := profile.GetBatched(context.TODO(), p.tier, reqs)
	if err != nil {
		return err
	}
	for i, v := range vals {
		row := rows[i]
		if v == nil {
			row.Set(colname, staticKwargs.GetUnsafe("default"))
		} else {
			row.Set(colname, v)
		}
		if err = out.Append(row); err != nil {
			return err
		}
	}
	return nil
}

func (p profileOp) Signature() *operators.Signature {
	return operators.NewSignature("profile", "addField").
		Input([]value.Type{value.Types.Dict}).
		Param("otype", value.Types.String, false, false, value.Nil).
		Param("oid", value.Types.Int, false, false, value.Nil).
		Param("key", value.Types.String, false, false, value.Nil).
		Param("version", value.Types.Int, false, true, value.Int(0)).
		Param("name", value.Types.String, true, false, value.Nil).
		Param("default", value.Types.Any, true, true, value.Nil)
}

var _ operators.Operator = profileOp{}
