package interpreter

import (
	"testing"

	"fennel/lib/value"

	"github.com/stretchr/testify/assert"
)

func TestEnv_Define_Lookup(t *testing.T) {
	env := NewEnv(nil)
	_, err := env.Lookup("var")
	assert.Error(t, err)
	var val value.Value = value.Int(1)
	err = env.Define("var", val)
	assert.NoError(t, err)
	ret, err := env.Lookup("var")
	assert.NoError(t, err)
	assert.Equal(t, val, ret)
	assert.Error(t, env.Define("var", value.Bool(true)))
	assert.Error(t, env.DefineReferencable("var", value.Bool(true)))
}

func TestEnv_DefineReferencable_Lookup(t *testing.T) {
	env := NewEnv(nil)
	_, err := env.Lookup("var")
	assert.Error(t, err)
	val := value.NewDict(map[string]value.Value{"foo": value.NewList(value.Int(1))})
	assert.NoError(t, env.DefineReferencable("var", val))
	ret, err := env.Lookup("var")
	assert.NoError(t, err)
	assert.Equal(t, val, ret)

	retd := ret.(value.Dict)
	val.Set("y", value.Int(1))
	found, ok := retd.Get("y")
	assert.True(t, ok)
	assert.Equal(t, value.Int(1), found)

	assert.Error(t, env.Define("var", value.Bool(true)))
	assert.Error(t, env.DefineReferencable("var", value.Bool(true)))
}

func TestEnv_Push_Pop(t *testing.T) {
	env := NewEnv(nil)
	var val value.Value = value.Int(1)
	err := env.Define("var", val)
	assert.NoError(t, err)
	ret, err := env.Lookup("var")
	assert.NoError(t, err)
	assert.Equal(t, val, ret)

	// Popping gives nil env back
	got, err := env.PopEnv()
	assert.NoError(t, err)
	assert.Equal(t, (*Env)(nil), got)

	// Cannot pop from a nil environment
	_, err = got.PopEnv()
	assert.Error(t, err)
}

func TestRedefine(t *testing.T) {
	env := NewEnv(nil)
	var oldval value.Value = value.Int(1)
	err := env.Define("var", oldval)
	assert.NoError(t, err)

	// Cannot redefine a variable in the same scope.
	err = env.Define("var", value.Bool(true))
	assert.Error(t, err)

	// Push a new env and then redefine.
	env2 := env.PushEnv()
	// For now, lookup still returns older value.
	ret, err := env2.Lookup("var")
	assert.NoError(t, err)
	assert.Equal(t, oldval, ret)
	newval := value.Bool(true)
	err = env2.Define("var", newval)
	assert.NoError(t, err)
	ret, err = env2.Lookup("var")
	assert.NoError(t, err)
	assert.Equal(t, newval, ret)

	// Set new variables in pushed env.
	var2 := "var2"
	val2 := value.Int(3)
	err = env2.Define(var2, val2)
	assert.NoError(t, err)
	ret, err = env2.Lookup(var2)
	assert.NoError(t, err)
	assert.Equal(t, val2, ret)

	// now, pop env and all this goes away
	fenv, err := env2.PopEnv()
	assert.NoError(t, err)
	assert.Equal(t, env, fenv)
	_, err = env.Lookup(var2)
	assert.Error(t, err)
	ret, err = fenv.Lookup("var")
	assert.NoError(t, err)
	assert.Equal(t, oldval, ret)
}
