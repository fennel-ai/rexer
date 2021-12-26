package interpreter

import (
	"engine/runtime"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEnv_Define_Lookup(t *testing.T) {
	env := Env{nil, make(map[string]runtime.Value)}
	ret, err := env.Lookup("var")
	assert.Error(t, err)
	var val runtime.Value = runtime.Int(1)
	env.Define("var", val)
	ret, err = env.Lookup("var")
	assert.Equal(t, val, ret)
	err = env.Define("var", runtime.Bool(true))
	assert.Error(t, err)

	// but can bypass this by calling redefine
	err = env.Redefine("var", runtime.Bool(true))
	assert.NoError(t, err)
	ret, err = env.Lookup("var")
	assert.Equal(t, runtime.Bool(true), ret)
}
