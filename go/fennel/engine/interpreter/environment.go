package interpreter

import (
	"fennel/value"
	"fmt"
)

type Env struct {
	parent *Env
	table  map[string]value.Value
}

func NewEnv() Env {
	return Env{nil, make(map[string]value.Value)}
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
		return ret, nil
	} else {
		return value.Nil, fmt.Errorf("undefined variable: '%s'", name)
	}
}
