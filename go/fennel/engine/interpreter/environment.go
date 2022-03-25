package interpreter

import (
	"fmt"

	"fennel/lib/value"
)

type Env struct {
	parent *Env
	table  map[string]value.Value
}

func NewEnv(parent *Env) Env {
	return Env{parent, make(map[string]value.Value)}
}

func (e *Env) Define(name string, value value.Value) error {
	if _, ok := e.table[name]; ok {
		return fmt.Errorf("re-defining symbol: '%s'", name)
	}
	e.table[name] = value
	return nil
}

// Redefine force sets the variable even if it involves redefinition
func (e *Env) Redefine(name string, value value.Value) error {
	e.table[name] = value
	return nil
}

func (e *Env) Lookup(name string) (value.Value, error) {
	if ret, ok := e.table[name]; ok {
		return ret.Clone(), nil
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
