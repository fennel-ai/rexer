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
	inputs map[string]reflect.Type
	params map[string]reflect.Type
}

func NewSignature() *Signature {
	return &Signature{
		make(map[string]reflect.Type, 0),
		make(map[string]reflect.Type, 0),
	}
}

func (s *Signature) Param(name string, t reflect.Type) *Signature {
	s.params[name] = t
	return s
}

func (s *Signature) Input(colname string, t reflect.Type) *Signature {
	s.inputs[colname] = t
	return s
}

type Operator interface {
	Apply(kwargs runtime.Dict, in runtime.Table, out *runtime.Table) error
	Signature() *Signature
}

func Validate(op Operator, kwargs runtime.Dict, schema map[string]reflect.Type) error {
	// first, let's validate params
	for k, t := range op.Signature().params {
		v, ok := kwargs[k]
		if !ok {
			return fmt.Errorf("operator '%s' expects kwarg '%s' but not found", op, k)
		}
		vt := reflect.TypeOf(v)
		if vt != t {
			return fmt.Errorf("type of  kwarg '%s' expected to be '%s' but found to be '%s'", k, t, vt)
		}
	}
	// next let's validate input table schema
	for k, t := range op.Signature().inputs {
		v, ok := schema[k]
		if !ok {
			return fmt.Errorf("operator '%s' expects col '%s' in input table but not found", op, k)
		}
		if v != t {
			return fmt.Errorf("type of  input column '%s' expected to be '%s' but found to be '%s'", k, t, v)
		}
	}
	return nil
}
