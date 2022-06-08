package operators

import (
	"errors"
	"fennel/lib/value"
)

type Kwargs struct {
	sig    *Signature
	static bool
	vals   []value.Value
}

func NewKwargs(sig *Signature, vals []value.Value, static bool) (Kwargs, error) {
	if static && len(sig.StaticKwargs) != len(vals) {
		return Kwargs{}, errors.New("length of static kwargs doesn't match the signature")
	} else if !static && len(sig.ContextKwargs) != len(vals) {
		return Kwargs{}, errors.New("length of context kwargs doesn't match the signature")
	}
	return Kwargs{
		sig:    sig,
		vals:   vals,
		static: static,
	}, nil
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
			if i < len(k.vals) {
				return k.vals[i], true
			} else {
				return nil, false
			}
		}
	}
	return nil, false
}

func (k *Kwargs) GetUnsafe(key string) value.Value {
	found, _ := k.Get(key)
	return found
}
