package profile

import (
	"context"
	"fmt"

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
	var reqs []libprofile.ProfileItem
	var rows []value.Value
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		rowVal := heads[0]
		req := libprofile.ProfileItem{
			OType:   ftypes.OType(kwargs.GetUnsafe("otype").(value.String)),
			Oid:     uint64(kwargs.GetUnsafe("oid").(value.Int)),
			Key:     string(kwargs.GetUnsafe("key").(value.String)),
			Version: uint64(kwargs.GetUnsafe("version").(value.Int)),
		}
		reqs = append(reqs, req)
		rows = append(rows, rowVal)
	}
	vals, err := profile.GetBatched(context.TODO(), p.tier, reqs)
	if err != nil {
		return err
	}
	field := string(staticKwargs.GetUnsafe("field").(value.String))
	defaultValue := staticKwargs.GetUnsafe("default")
	for i, v := range vals {
		if v == nil {
			v = defaultValue
		}
		var outRow value.Value
		if len(field) > 0 {
			if d, ok := rows[i].(value.Dict); !ok {
				return fmt.Errorf("input values should be dict with field for profile operator is non-empty")
			} else {
				d.Set(field, v)
				outRow = d
			}
		} else {
			outRow = v
		}
		if err = out.Append(outRow); err != nil {
			return err
		}
	}
	return nil
}

func (p profileOp) Signature() *operators.Signature {
	return operators.NewSignature("std", "profile").
		Input([]value.Type{value.Types.Any}).
		Param("otype", value.Types.String, false, false, value.Nil).
		Param("oid", value.Types.Int, false, false, value.Nil).
		Param("key", value.Types.String, false, false, value.Nil).
		Param("version", value.Types.Int, false, true, value.Int(0)).
		Param("field", value.Types.String, true, true, value.String("")).
		Param("default", value.Types.Any, true, true, value.Nil)
}

var _ operators.Operator = profileOp{}
