package operators

import (
	"encoding/json"
	"fmt"

	"fennel/lib/value"
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

func IsMapper(op Operator) bool {
	return op.Signature().Mapper
}

type Param struct {
	Name     string
	Static   bool
	Type     value.Type
	Optional bool
	Default  value.Value
}

type Signature struct {
	Module        string
	Name          string
	input         value.Type
	StaticKwargs  map[string]Param
	ContextKwargs map[string]Param
	Mapper        bool
}

func NewSignature(module, name string, mapper bool) *Signature {
	return &Signature{
		module, name,
		value.Types.Any,
		make(map[string]Param, 0),
		make(map[string]Param, 0),
		mapper,
	}
}

func (s *Signature) Param(name string, t value.Type, static bool, optional bool, default_ value.Value) *Signature {
	p := Param{name, static, t, optional, default_}
	if static {
		s.StaticKwargs[name] = p

	} else {
		s.ContextKwargs[name] = p
	}
	return s
}

func (s *Signature) Input(t value.Type) *Signature {
	s.input = t
	return s
}

type Operator interface {
	New(args value.Dict, bootargs map[string]interface{}) (Operator, error)
	Apply(kwargs value.Dict, in InputIter, out *value.List) error
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

func GetOperatorsJSON() ([]byte, error) {
	type param struct {
		Type     string `json:"Type"`
		Optional bool   `json:"Optional"`
	}
	opdata := make(map[string]map[string]map[string]param)
	for module, ops := range registry {
		opdata[module] = make(map[string]map[string]param)
		for fname, op := range ops {
			opdata[module][fname] = make(map[string]param)
			sig := op.Signature()
			for _, p := range sig.ContextKwargs {
				opdata[module][fname][p.Name] = param{
					Type:     p.Type.String(),
					Optional: p.Optional,
				}
			}
			for _, p := range sig.StaticKwargs {
				opdata[module][fname][p.Name] = param{
					Type:     p.Type.String(),
					Optional: p.Optional,
				}
			}
		}
	}
	return json.Marshal(opdata)
}

func TypeCheckStaticKwargs(op Operator, staticKwargs value.Dict) error {
	sig := op.Signature()
	if len(sig.StaticKwargs) != len(staticKwargs) {
		return fmt.Errorf("[%s.%s] incorrect number of static kwargs passed - expected: %d but got: %d",
			sig.Module, sig.Name, len(sig.StaticKwargs), len(staticKwargs))
	}
	for k, p := range sig.StaticKwargs {
		v, ok := staticKwargs[k]
		if !ok {
			return fmt.Errorf("operator '%s' expects kwarg '%s' but not found", op, k)
		}
		if err := p.Type.Validate(v); err != nil {
			return fmt.Errorf("type of kwarg '%s' is not of type '%s': %s", k, p.Type, err)
		}
	}
	return nil
}

func Typecheck(op Operator, inputVal value.Value, contextKwargs value.Dict) error {
	sig := op.Signature()
	// let's look at contextual kwargs first
	if len(sig.ContextKwargs) != len(contextKwargs) {
		return fmt.Errorf("[%s.%s] incorrect number of contextual kwargs passed - expected: %d but got: %d",
			sig.Module, sig.Name, len(sig.ContextKwargs), len(contextKwargs))
	}
	for k, p := range sig.ContextKwargs {
		v, ok := contextKwargs[k]
		if !ok {
			return fmt.Errorf("operator '%s.%s' expects kwarg '%s' but not found", sig.Module, sig.Name, k)
		}
		if err := p.Type.Validate(v); err != nil {
			return fmt.Errorf("type of kwarg '%s'is not of type '%s': %s", k, p.Type, err)
		}
	}
	// next let's validate input
	if err := sig.input.Validate(inputVal); err != nil {
		return fmt.Errorf("element of input list to operator '%s.%s' is not of type '%s': %s",
			sig.Module, sig.Name, sig.input, err)
	}
	return nil
}

type InputIter = ZipIter
