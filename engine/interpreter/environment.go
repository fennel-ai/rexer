package interpreter

import (
	"engine/runtime"
	"fmt"
)

type Env struct {
	parent *Env
	table  map[string]runtime.Value
}

func NewEnv() Env {
	return Env{nil, make(map[string]runtime.Value)}
}

func (e *Env) Define(name string, value runtime.Value) error {
	if _, ok := e.table[name]; ok {
		return fmt.Errorf("re-defining symbol: '%s'", name)
	}
	e.table[name] = value
	return nil
}

// Redefine force sets the variable even if it involves redefinition
func (e *Env) Redefine(name string, value runtime.Value) error {
	e.table[name] = value
	return nil
}

func (e *Env) Lookup(name string) (runtime.Value, error) {
	if ret, ok := e.table[name]; ok {
		return ret, nil
	} else {
		return runtime.Nil, fmt.Errorf("undefined variable: '%s'", name)
	}
}
