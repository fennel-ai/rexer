package opdefs

import (
	"fennel/controller/profile"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	"fennel/lib/ftypes"
	libprofile "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/tier"
)

func init() {
	operators.Register(&profileOp{})
}

type profileOp struct {
	tier tier.Tier
}

func (p *profileOp) Init(args value.Dict, bootargs map[string]interface{}) error {
	var err error
	if p.tier, err = bootarg.GetTier(bootargs); err != nil {
		return err
	}
	return nil
}

func (p *profileOp) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.Table) error {
	colname := string(staticKwargs["name"].(value.String))
	reqs := make([]libprofile.ProfileItem, 0)
	rows := make([]value.Dict, 0)
	for in.HasMore() {
		row, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		req := libprofile.ProfileItem{
			OType:   ftypes.OType(kwargs["otype"].(value.String)),
			Oid:     uint64(kwargs["oid"].(value.Int)),
			Key:     string(kwargs["key"].(value.String)),
			Version: uint64(kwargs["version"].(value.Int)),
		}
		reqs = append(reqs, req)
		rows = append(rows, row)
	}
	vals, err := profile.GetBatched(p.tier, reqs)
	if err != nil {
		return err
	}
	for i, v := range vals {
		row := rows[i]
		if v == nil {
			row[colname] = staticKwargs["default"]
		} else {
			row[colname] = v
		}
		if err = out.Append(row); err != nil {
			return err
		}
	}
	return nil
}

func (p *profileOp) Signature() *operators.Signature {
	return operators.NewSignature(p, "profile", "addField").
		Param("otype", value.Types.String, false, false, value.Nil).
		Param("oid", value.Types.Int, false, false, value.Nil).
		Param("key", value.Types.String, false, false, value.Nil).
		Param("version", value.Types.Int, false, true, value.Int(0)).
		Param("name", value.Types.String, true, false, value.Nil).
		Param("default", value.Types.Any, true, true, value.Nil)
}

var _ operators.Operator = &profileOp{}
