package std

import (
	"fennel/engine/operators"
	"fennel/lib/value"
	"fmt"
)

type ExplodeOperator struct{}

var _ operators.Operator = ExplodeOperator{}

func (e ExplodeOperator) New(_ value.Dict, _ map[string]interface{}) (operators.Operator, error) {
	return ExplodeOperator{}, nil
}

func (e ExplodeOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "explode", true).
		Param("keys", value.Types.Any, true, false, value.Nil).
		Input(value.Types.Dict)
}

func (e ExplodeOperator) Apply(staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	keys := staticKwargs["keys"]
	for in.HasMore() {
		row, _, err := in.Next()
		if err != nil {
			return err
		}
		rowVal := row.(value.Dict)

		// `keys` are either a string (e.g. `keys='foo'``) or list of strings (e.g. `keys=['foo', 'bar']`)
		switch keys := keys.(type) {
		case value.String:
			kstr, err := validateKey(keys, rowVal)
			if err != nil {
				return err
			}
			// if the value type is a list, explode it. else just set it as is
			vs, ok := rowVal[kstr].(value.List)
			if !ok {
				out.Append(rowVal)
			} else {
				// if the list is empty, write `Nil`
				if len(vs) == 0 {
					newRow := rowVal.Clone().(value.Dict)
					newRow[kstr] = value.Nil
					out.Append(newRow)
				} else {
					for _, v := range vs {
						newRow := rowVal.Clone().(value.Dict)
						newRow[kstr] = v
						out.Append(newRow)
					}
				}
			}
		case value.List:
			// provided a list of keys, the length of each list-like row entry should match
			// if the values are scalar, they are written as-is
			if len(keys) == 0 {
				return fmt.Errorf("list of keys provided should not be empty")
			}
			kstr, err := validateKey(keys[0], rowVal)
			if err != nil {
				return err
			}
			// every list-like entry should match in length, otherwise write the information as-is
			// in case of empty lists, write the value.Nil
			expectedLength := -1
			if vs, ok := rowVal[kstr].(value.List); ok {
				expectedLength = len(vs)
			}
			for i := 1; i < len(keys); i++ {
				kstr, err := validateKey(keys[i], rowVal)
				if err != nil {
					return err
				}
				if vs, ok := rowVal[kstr].(value.List); ok {
					if expectedLength != len(vs) {
						return fmt.Errorf("columns must have matching element counts. "+
							"Given: %d, %d", expectedLength, len(vs))
					}
				} else {
					if expectedLength != -1 {
						return fmt.Errorf("comparing list with a scalar; columns must have matching element counts")
					}
				}
			}

			if expectedLength == -1 {
				// write as-is
				out.Append(rowVal)
			} else if expectedLength == 0 {
				newRow := rowVal.Clone().(value.Dict)
				for ki := 0; ki < len(keys); ki++ {
					kstr := string(keys[ki].(value.String))
					newRow[kstr] = value.Nil
				}
				out.Append(newRow)
			} else {
				// explode each key
				for i := 0; i < expectedLength; i++ {
					newRow := rowVal.Clone().(value.Dict)
					for ki := 0; ki < len(keys); ki++ {
						kstr := string(keys[ki].(value.String))
						vs := rowVal[kstr].(value.List)
						newRow[kstr] = vs[i]
					}
					out.Append(newRow)
				}
			}
		default:
			return fmt.Errorf("key(s) provided must be a list of keys or a key string, "+
				"cannot be of type: %+v", keys)
		}
	}
	return nil
}

func validateKey(key value.Value, rowVal value.Dict) (string, error) {
	k, ok := key.(value.String)
	if !ok {
		return "", fmt.Errorf("key must be a string, given: %+v", key)
	}
	kstr := string(k)
	_, ok = rowVal[kstr]
	if !ok {
		return "", fmt.Errorf("key: %s is invalid", kstr)
	}
	return kstr, nil
}
