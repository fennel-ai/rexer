package std

import (
	"context"
	"fmt"
	"sync"

	"fennel/engine/operators"
	"fennel/lib/value"
)

type ExplodeOperator struct{}

var _ operators.Operator = ExplodeOperator{}

func (e ExplodeOperator) New(
	args value.Dict, bootargs map[string]interface{}, cache *sync.Map,
) (operators.Operator, error) {
	return ExplodeOperator{}, nil
}

func (e ExplodeOperator) Signature() *operators.Signature {
	return operators.NewSignature("std", "explode").
		ParamWithHelp("field", value.Types.Any, true, false, value.Nil,
			"StaticKwarg: field is either a string (e.g. `field='foo'``) or list of strings (e.g. `field=['foo', 'bar']` based on which a single row is broken into multiple rows each with a unique value of field").
		Input([]value.Type{value.Types.Dict})
}

func (e ExplodeOperator) Apply(_ context.Context, staticKwargs value.Dict, in operators.InputIter, out *value.List) error {
	field, _ := staticKwargs.Get("field")
	for in.HasMore() {
		rows, _, err := in.Next()
		if err != nil {
			return err
		}
		row := rows[0]
		rowVal := row.(value.Dict)

		// `field` are either a string (e.g. `field='foo'``) or list of strings (e.g. `field=['foo', 'bar']`)
		switch keys := field.(type) {
		case value.String:
			kstr, err := validateKey(keys, rowVal)
			if err != nil {
				return err
			}
			// if the value type is a list, explode it. else just set it as is
			val, _ := rowVal.Get(kstr)
			vs, ok := val.(value.List)
			if !ok {
				out.Append(rowVal)
			} else {
				// if the list is empty, write `Nil`
				if vs.Len() == 0 {
					newRow := rowVal.Clone().(value.Dict)
					newRow.Set(kstr, value.Nil)
					out.Append(newRow)
				} else {
					out.Grow(vs.Len())
					for i := 0; i < vs.Len(); i++ {
						v, _ := vs.At(i)
						newRow := rowVal.Clone().(value.Dict)
						newRow.Set(kstr, v)
						out.Append(newRow)
					}
				}
			}
		case value.List:
			// provided a list of field, the length of each list-like row entry should match
			// if the values are scalar, they are written as-is
			if keys.Len() == 0 {
				return fmt.Errorf("list of field provided should not be empty")
			}
			k, _ := keys.At(0)
			kstr, err := validateKey(k, rowVal)
			if err != nil {
				return err
			}
			// every list-like entry should match in length, otherwise write the information as-is
			// in case of empty lists, write the value.Nil
			expectedLength := -1
			val, _ := rowVal.Get(kstr)
			if vs, ok := val.(value.List); ok {
				expectedLength = vs.Len()
			}
			for i := 1; i < keys.Len(); i++ {
				k, _ := keys.At(i)
				kstr, err := validateKey(k, rowVal)
				if err != nil {
					return err
				}
				val, _ := rowVal.Get(kstr)
				if vs, ok := val.(value.List); ok {
					if expectedLength != vs.Len() {
						return fmt.Errorf("columns must have matching element counts. "+
							"Given: %d, %d", expectedLength, vs.Len())
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
				for ki := 0; ki < keys.Len(); ki++ {
					k, _ := keys.At(ki)
					kstr := string(k.(value.String))
					newRow.Set(kstr, value.Nil)
				}
				out.Append(newRow)
			} else {
				// explode each key
				out.Grow(expectedLength)
				for i := 0; i < expectedLength; i++ {
					newRow := rowVal.Clone().(value.Dict)
					for ki := 0; ki < keys.Len(); ki++ {
						k, _ := keys.At(ki)
						kstr := string(k.(value.String))
						val, _ := rowVal.Get(kstr)
						vs := val.(value.List)
						v, _ := vs.At(i)
						newRow.Set(kstr, v)
					}
					out.Append(newRow)
				}
			}
		default:
			return fmt.Errorf("key(s) provided must be a list of field or a key string, "+
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
	_, ok = rowVal.Get(kstr)
	if !ok {
		return "", fmt.Errorf("key: %s is invalid", kstr)
	}
	return kstr, nil
}
