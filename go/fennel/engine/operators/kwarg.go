package operators

import (
	"fmt"

	"fennel/lib/value"
)

type Kwargs struct {
	sig    *Signature
	static bool
	vals   []value.Value
}

func NewKwargs(sig *Signature, vals []value.Value, static bool) (Kwargs, error) {
	kw := Kwargs{
		sig:    sig,
		vals:   vals,
		static: static,
	}
	return kw, kw.TypeCheck()
}

func (k *Kwargs) TypeCheck() error {
	var params []Param
	if k.static {
		params = k.sig.StaticKwargs
	} else {
		params = k.sig.ContextKwargs
	}
	if len(params) != len(k.vals) {
		return fmt.Errorf("length of static/contextual kwargs doesn't match the signature for %s.%s. Expected: %d, Got: %d", k.sig.Module, k.sig.Name, len(params), len(k.vals))
	}
	for i, p := range params {
		v := k.vals[i]
		// allow value.Nil to be used for optional params
		if p.Optional && v == value.Nil {
			continue
		}
		if err := p.Type.Validate(v); err != nil {
			return fmt.Errorf("operator '%s.%s' expects type of kwarg '%s' to be of type '%s': %w", k.sig.Module, k.sig.Name, p.Name, p.Type, err)
		}
	}
	return nil
}

func (k *Kwargs) Len() int {
	return len(k.vals)
}

func (k *Kwargs) Get(key string) (value.Value, bool) {
	var params []Param
	if k.static {
		params = k.sig.StaticKwargs
	} else {
		params = k.sig.ContextKwargs
	}
	for i, p := range params {
		if p.Name == key {
			return k.vals[i], true
		}
	}
	return nil, false
}

func (k *Kwargs) GetUnsafe(key string) value.Value {
	found, _ := k.Get(key)
	return found
}
