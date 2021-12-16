package runtime

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEnv_Define_Lookup(t *testing.T) {
	env := Env{nil, make(map[string]Value)}
	ret, err := env.Lookup("var")
	assert.Error(t, err)
	var val Value = Int(1)
	env.Define("var", val)
	ret, err = env.Lookup("var")
	assert.Equal(t, val, ret)
	err = env.Define("var", Bool(true))
	assert.Error(t, err)

	// but can bypass this by calling redefine
	err = env.Redefine("var", Bool(true))
	assert.NoError(t, err)
	ret, err = env.Lookup("var")
	assert.Equal(t, Bool(true), ret)
}
