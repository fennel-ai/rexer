package rename

import (
	"context"
	"fmt"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func init() {
	operators.Register(renamer{})
}

type renamer struct{}

func (r renamer) New(
	args value.Dict, bootargs map[string]interface{},
) (operators.Operator, error) {
	return renamer{}, nil
}

func (r renamer) Apply(_ context.Context, kwargs value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		data := heads[0].(value.Dict)
		from_ := string(kwargs.GetUnsafe("field").(value.String))
		to_ := string(kwargs.GetUnsafe("to").(value.String))
		val, ok := data.Get(from_)
		if !ok {
			return fmt.Errorf("input dict doesn't have field: %s", from_)
		}
		data.Set(to_, val)
		data.Del(from_)
		out.Append(data)
	}
	return nil
}

func (r renamer) Signature() *operators.Signature {
	return operators.NewSignature("std", "rename").
		Input([]value.Type{value.Types.Dict}).
		ParamWithHelp("field", value.Types.String, false, false, nil, "old name of the field (as string) that needs to be renamed").
		ParamWithHelp("to", value.Types.String, false, false, nil, "new name (as string) that the field will be renamed to")
}

var _ operators.Operator = renamer{}
