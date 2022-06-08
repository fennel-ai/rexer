package interpreter

import (
	"fmt"
	"sync"

	"fennel/lib/value"
)

var (
	evPool = sync.Pool{
		New: func() interface{} {
			return &envValue{}
		},
	}
	envPool = sync.Pool{
		New: func() interface{} {
			return &Env{
				table: make(map[string]*envValue),
			}
		},
	}
)

type envValue struct {
	value  value.Value
	useRef bool
}

type Env struct {
	parent *Env
	table  map[string]*envValue
}

func NewEnv(parent *Env) *Env {
	e := envPool.Get().(*Env)
	e.parent = parent
	return e
}

func (e *Env) define(name string, value value.Value, useRef bool) error {
	if _, ok := e.table[name]; ok {
		return fmt.Errorf("re-defining symbol: '%s'", name)
	}
	ev := evPool.Get().(*envValue)
	ev.useRef = useRef
	ev.value = value
	e.table[name] = ev
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
	re := NewEnv(e)
	return re
}

// PopEnv removes the outermost environment to return the parent environment
func (e *Env) PopEnv() (*Env, error) {
	if e == nil {
		return nil, fmt.Errorf("can not pop nil environment")
	}
	p := e.parent
	for k, ev := range e.table {
		delete(e.table, k)
		// Reset value to nil to avoid memory sitting around in the pool.
		// Not setting value to Nil would not necessary cause a leak, but it
		// helps in making the underlying value available for GC sooner.
		ev.value = value.Nil
		evPool.Put(ev)
	}
	e.parent = nil
	envPool.Put(e)
	return p, nil
}
