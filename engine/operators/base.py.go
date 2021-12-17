package operators

import (
	"engine/runtime"
	"fmt"
	"reflect"
)

func init() {
	registry = make(map[string]map[string]Operator, 0)
}

type Registry = map[string]map[string]Operator

var registry Registry

func Locate(namespace, name string) (Operator, error) {
	if ns, ok := registry[namespace]; !ok {
		return nil, fmt.Errorf("unregistered operator namespace: '%s'", namespace)
	} else {
		if ret, ok := ns[name]; !ok {
			return nil, fmt.Errorf("unregistered operator '%s' in namespace: '%s'", name, namespace)
		} else {
			return ret, nil
		}
	}
}

type Signature struct {
	Params map[string]reflect.Type
}

type Operator interface {
	Apply(kwargs runtime.Dict, in runtime.Table, out *runtime.Table) error
	Signature() Signature
}

func Validate(op Operator, kwargs runtime.Dict) error {
	for k, t := range op.Signature().Params {
		v, ok := kwargs[k]
		if !ok {
			return fmt.Errorf("operator '%s' expects kwarg '%s' but not found", op, k)
		}
		vt := reflect.TypeOf(v)
		if vt != t {
			return fmt.Errorf("type of  kwarg '%s' expected to be '%s' but found to be '%s'", k, t, vt)
		}
	}
	return nil
}
