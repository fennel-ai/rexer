package operators

import (
	"context"
	"encoding/json"
	"fmt"

	"fennel/lib/value"
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

type Param struct {
	Name     string
	Static   bool
	Type     value.Type
	Optional bool
	Default  value.Value
	Help     string
}

type Signature struct {
	Module string
	Name   string
	// type of each input
	// a zero length list means any number of inputs with any types are allowed
	// default value is a single element list with type of 'Any'
	InputTypes    []value.Type
	StaticKwargs  []Param
	ContextKwargs []Param
}

func NewSignature(module, name string) *Signature {
	return &Signature{
		module, name,
		[]value.Type{value.Types.Any},
		[]Param{},
		[]Param{},
	}
}

func (s *Signature) Param(name string, t value.Type, static bool, optional bool, default_ value.Value) *Signature {
	return s.ParamWithHelp(name, t, static, optional, default_, "")
}

func (s *Signature) ParamWithHelp(name string, t value.Type, static bool, optional bool, default_ value.Value, help string) *Signature {
	p := Param{name, static, t, optional, default_, help}
	if static {
		s.StaticKwargs = append(s.StaticKwargs, p)
		// s.StaticKwargs[name] = p

	} else {
		s.ContextKwargs = append(s.ContextKwargs, p)
		// s.ContextKwargs[name] = p
	}
	return s
}

func (s *Signature) Input(types []value.Type) *Signature {
	s.InputTypes = make([]value.Type, len(types))
	copy(s.InputTypes, types)
	return s
}

type Operator interface {
	New(args value.Dict, bootargs map[string]interface{}) (Operator, error)
	Apply(ctx context.Context, kwargs Kwargs, in InputIter, out *value.List) error
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

type param struct {
	Type     string `json:"Type"`
	Optional bool   `json:"Optional"`
	Help     string `json:"Help"`
}

func GetOperatorsJSON() ([]byte, error) {
	return json.Marshal(GetOperators())
}

func GetOperators() map[string]map[string]map[string]param {
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
					Help:     p.Help,
				}
			}
			for _, p := range sig.StaticKwargs {
				opdata[module][fname][p.Name] = param{
					Type:     p.Type.String(),
					Optional: p.Optional,
					Help:     p.Help,
				}
			}
		}
	}
	return opdata
}

func ValidateInputs(sig *Signature, inputVal []value.Value) error {
	// Validate InputTypes
	if len(sig.InputTypes) > 0 {
		if len(inputVal) != len(sig.InputTypes) {
			return fmt.Errorf("operator '%s.%s' expects '%d' inputs but received '%d' inputs", sig.Module, sig.Name, len(sig.InputTypes), len(inputVal))
		}
		for i := 0; i < len(inputVal); i++ {
			if err := sig.InputTypes[i].Validate(inputVal[i]); err != nil {
				return fmt.Errorf("input # '%d' for operator '%s.%s' not found to be of expected type '%s': %s",
					i, sig.Module, sig.Name, sig.InputTypes[i], err)
			}
		}
	}
	return nil
}

type InputIter = ZipIter
