package interpreter

import (
	"fmt"

	"fennel/lib/value"
)

type envValue struct {
	value  value.Value
	useRef bool
}

type Env struct {
	parent *Env
	table  map[string]envValue
}

func NewEnv(parent *Env) *Env {
	return &Env{
		parent: parent,
		table:  make(map[string]envValue),
	}
}

func (e *Env) define(name string, value value.Value, useRef bool) error {
	if _, ok := e.table[name]; ok {
		return fmt.Errorf("re-defining symbol: '%s'", name)
	}
	e.table[name] = envValue{value, useRef}
	return nil
}

func (e *Env) DefineReferencable(name string, value value.Value) error {
	return e.define(name, value, true /*useRef=*/)
}

func (e *Env) Define(name string, value value.Value) error {
	return e.define(name, value, false /*useRef=*/)
}

func (e *Env) Lookup(name string) (value.Value, error) {
	if ret, ok := e.table[name]; ok {
		if ret.useRef {
			return ret.value, nil
		} else {
			return ret.value.Clone(), nil
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
	return NewEnv(e)
}

// PopEnv removes the outermost environment to return the parent environment
func (e *Env) PopEnv() (*Env, error) {
	if e == nil {
		return nil, fmt.Errorf("can not pop nil environment")
	}
	return e.parent, nil
}
