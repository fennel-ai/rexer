package profile

import (
	"fennel/engine/operators"
	"fennel/instance"
	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/value"
	"fmt"
)

func init() {
	operators.Register(&profileOp{})
}

type profileOp struct {
	instance instance.Instance
}

func (p *profileOp) Init(args value.Dict, bootargs map[string]interface{}) error {
	got, ok := bootargs["__instance__"]
	if !ok {
		return fmt.Errorf("instance not provided in bootargs")
	}
	p.instance, ok = got.(instance.Instance)
	if !ok {
		return fmt.Errorf("bootargs key __instance__ contains: '%v', not an instance", got)
	}
	return nil
}

func (p *profileOp) Apply(_ value.Dict, in operators.InputIter, out *value.Table) error {
	for in.HasMore() {
		row, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		req := profile.ProfileItem{
			CustID:  p.instance.CustID,
			OType:   ftypes.OType(kwargs["otype"].(value.String)),
			Oid:     uint64(kwargs["oid"].(value.Int)),
			Key:     string(kwargs["key"].(value.String)),
			Version: uint64(kwargs["version"].(value.Int)),
		}
		if row["profile_value"], err = Get(p.instance, req); err != nil {
			return err
		}
		if err = out.Append(row); err != nil {
			return err
		}
	}
	return nil
}

func (p *profileOp) Signature() *operators.Signature {
	return operators.NewSignature(p, "std", "addProfileColumn").
		Param("otype", value.Types.String, false, false, value.Nil).
		Param("oid", value.Types.Int, false, false, value.Nil).
		Param("key", value.Types.String, false, false, value.Nil).
		Param("version", value.Types.Int, false, true, value.Int(0))
}

var _ operators.Operator = &profileOp{}
