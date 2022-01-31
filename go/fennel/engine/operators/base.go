package operators

import (
	"fennel/lib/utils"
	"fennel/lib/value"
	"fmt"
	"reflect"
)

func init() {
	registry = make(map[string]map[string]Operator, 0)
}

type Registry = map[string]map[string]Operator

var registry Registry

// TODO: how do we create multiple structs for each operator to avoid sharing state
// in a single request
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

type Param struct {
	Name     string
	Static   bool
	Type     reflect.Type
	Optional bool
	Default  value.Value
}

// TODO: support optional parameters with default values
type Signature struct {
	Module        string
	Name          string
	inputs        map[string]reflect.Type
	StaticKwargs  map[string]Param
	ContextKwargs map[string]Param
}

func NewSignature(op Operator, module, name string) *Signature {
	return &Signature{
		module, name,
		make(map[string]reflect.Type, 0),
		make(map[string]Param, 0),
		make(map[string]Param, 0),
	}
}

func (s *Signature) Param(name string, t reflect.Type, static bool, optional bool, default_ value.Value) *Signature {
	p := Param{name, static, t, optional, default_}
	if static {
		s.StaticKwargs[name] = p

	} else {
		s.ContextKwargs[name] = p
	}
	return s
}

func (s *Signature) Input(colname string, t reflect.Type) *Signature {
	s.inputs[colname] = t
	return s
}

type Operator interface {
	Init(args value.Dict, bootargs map[string]interface{}) error
	Apply(kwargs value.Dict, in InputIter, out *value.Table) error
	Signature() *Signature
}

func Register(op Operator) error {
	sig := op.Signature()
	module, name := sig.Module, sig.Name
	if _, ok := registry[module]; !ok {
		registry[module] = make(map[string]Operator)
	}
	if _, ok := registry[module][name]; ok {
		return fmt.Errorf("can not register operator: module: '%s' & name: '%s' already taken", module, name)
	}
	registry[module][name] = op
	return nil
}

func Typecheck(op Operator, staticKwargs map[string]reflect.Type, inputSchema map[string]reflect.Type, contextKwargSchema map[string]reflect.Type) error {
	// first, let's validate static kwargs
	sig := op.Signature()
	if len(sig.StaticKwargs) != len(staticKwargs) {
		return fmt.Errorf("incorrect number of static kwargs passed - expected: %d but got: %d", len(sig.StaticKwargs), len(staticKwargs))
	}
	for k, p := range sig.StaticKwargs {
		t := p.Type
		vt, ok := staticKwargs[k]
		if !ok {
			return fmt.Errorf("operator '%s' expects kwarg '%s' but not found", op, k)
		}
		if t != value.Types.Any && vt != t {
			return fmt.Errorf("type of  kwarg '%s' expected to be '%s' but found to be '%s'", k, t, vt)
		}
	}
	// next, let's look at contextual kwargs
	if len(sig.ContextKwargs) != len(contextKwargSchema) {
		return fmt.Errorf("incorrect number of contextual kwargs passed - expected: %d but got: %d", len(sig.ContextKwargs), len(contextKwargSchema))
	}
	for k, p := range sig.ContextKwargs {
		t := p.Type
		vt, ok := contextKwargSchema[k]
		if !ok {
			return fmt.Errorf("operator '%s.%s' expects kwarg '%s' but not found", sig.Module, sig.Name, k)
		}
		if t != value.Types.Any && vt != t {
			return fmt.Errorf("type of kwarg '%s' expected to be '%s' but found to be '%s'", k, t, vt)
		}
	}
	// next let's validate input table inputSchema
	for k, t := range sig.inputs {
		vt, ok := inputSchema[k]
		if !ok {
			return fmt.Errorf("operator '%s.%s' expects col '%s' in input table but not found", sig.Module, sig.Name, k)
		}
		if vt != t {
			return fmt.Errorf("type of  input column '%s' expected to be '%s' but found to be '%s'", k, t, vt)
		}
	}
	return nil
}

type InputIter = utils.ZipIter
