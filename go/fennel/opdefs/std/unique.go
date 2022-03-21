package std

import (
	"fennel/engine/operators"
	"fennel/lib/value"
)

type UniqueOperator struct{}

var _ operators.Operator = UniqueOperator{}

func (op UniqueOperator) New(_ value.Dict, _ map[string]interface{}) (operators.Operator, error) {
	return UniqueOperator{}, nil
}

func (op UniqueOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "unique", false).
		Param("name", value.Types.String /*t=*/, true /*static=*/, false /*optional=*/, value.Nil).
		Input(value.Types.Dict)
}

func (op UniqueOperator) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	n, _ := staticKwargs.Get("name")
	name := string(n.(value.String))
	for in.HasMore() {
		r, _, err := in.Next()
		if err != nil {
			return err
		}
		row := r.(value.Dict)
		if values, ok := row.Get(name); ok {
			valToVisited := make(map[string]struct{})
			switch v := values.(type) {
			case value.List:
				var vals value.List
				for i := 0; i < v.Len(); i++ {
					val, _ := v.At(i)
					if _, found := valToVisited[val.String()]; !found {
						valToVisited[val.String()] = struct{}{}
						vals.Append(val)
						//vals = append(vals, val)
					}
				}
				row.Set(name, vals)
			}
		}
		out.Append(row)
	}
	return nil
}
