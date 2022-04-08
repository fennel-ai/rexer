package interpreter

import (
	"fmt"

	"fennel/lib/value"
)

type Env struct {
	parent *Env
	table  map[string]value.Value
	useRef map[string]bool
}

func NewEnv(parent *Env) Env {
	return Env{parent, make(map[string]value.Value), make(map[string]bool)}
}

func (e *Env) define(name string, value value.Value, useRef bool) error {
	if _, ok := e.table[name]; ok {
		return fmt.Errorf("re-defining symbol: '%s'", name)
	}
	e.table[name] = value
	e.useRef[name] = useRef
	return nil
}

func (e *Env) DefineReferencable(name string, value value.Value) error {
	return e.define(name, value, true /*useRef=*/)
}

func (e *Env) Define(name string, value value.Value) error {
	return e.define(name, value, false /*useRef=*/)
}

func (e *Env) Lookup(name string) (value.Value, error) {
	ret, ok := e.table[name]
	if ok {
		ref, ok := e.useRef[name]
		if !ok {
			return value.Nil, fmt.Errorf("variable defined, type unknown: '%s'", name)
		}
		if ref {
			return ret, nil
		}
		return ret.Clone(), nil
	} else {
		// should not exist in the `useRef` map as well
		if _, ok := e.useRef[name]; ok {
			return value.Nil, fmt.Errorf("variable undefined, but type known: '%s'", name)
		}
	}

	if e.parent == nil {
		return value.Nil, fmt.Errorf("undefined variable: '%s'", name)
	} else {
		return e.parent.Lookup(name)
	}
}

// PushEnv creates an environment that is child of the caller
func (e *Env) PushEnv() *Env {
	ret := NewEnv(e)
	return &ret
}

// PopEnv removes the outermost environment to return the parent environment
func (e *Env) PopEnv() (*Env, error) {
	if e == nil {
		return nil, fmt.Errorf("can not pop nil environment")
	}
	return e.parent, nil
}
