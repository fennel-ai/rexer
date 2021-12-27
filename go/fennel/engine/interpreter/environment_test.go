package interpreter

import (
	"fennel/value"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnv_Define_Lookup(t *testing.T) {
	env := Env{nil, make(map[string]value.Value)}
	ret, err := env.Lookup("var")
	assert.Error(t, err)
	var val value.Value = value.Int(1)
	env.Define("var", val)
	ret, err = env.Lookup("var")
	assert.Equal(t, val, ret)
	err = env.Define("var", value.Bool(true))
	assert.Error(t, err)

	// but can bypass this by calling redefine
	err = env.Redefine("var", value.Bool(true))
	assert.NoError(t, err)
	ret, err = env.Lookup("var")
	assert.Equal(t, value.Bool(true), ret)
}
