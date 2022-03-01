package value

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func verifyMarshalUnMarshal(t *testing.T, v Value) {
	b, err := Marshal(v)
	assert.NoError(t, err)
	var u Value
	err = Unmarshal(b, &u)
	assert.NoError(t, err)
	assert.Equal(t, v, u)
}

func verifyUnequalMarshal(t *testing.T, v Value, unequal []Value) {
	b, err := Marshal(v)
	assert.NoError(t, err)
	var u Value
	err = Unmarshal(b, &u)
	for _, other := range unequal {
		assert.NotEqual(t, u, other)
	}
}

func TestEqualMarshal(t *testing.T) {
	verifyMarshalUnMarshal(t, Int(0))
	verifyMarshalUnMarshal(t, Int(120))
	verifyMarshalUnMarshal(t, Int(100000000))
	verifyMarshalUnMarshal(t, Int(141414141))
	verifyMarshalUnMarshal(t, Int(-411414141))
	verifyMarshalUnMarshal(t, Double(0.1))
	verifyMarshalUnMarshal(t, Double(-0.1))
	verifyMarshalUnMarshal(t, Double(0))
	verifyMarshalUnMarshal(t, Double(1e-4))
	verifyMarshalUnMarshal(t, Double(1e14))
	verifyMarshalUnMarshal(t, Bool(true))
	verifyMarshalUnMarshal(t, Bool(false))
	verifyMarshalUnMarshal(t, String(""))
	verifyMarshalUnMarshal(t, String("hi"))
	verifyMarshalUnMarshal(t, String("i12_%2342]{"))
	values := []Value{Int(1), Int(2), String("here"), Bool(false), Nil}
	list := List(values)
	verifyMarshalUnMarshal(t, list)
	verifyMarshalUnMarshal(t, List([]Value{}))

	verifyMarshalUnMarshal(t, Nil)

	kwargs := make(map[string]Value, 0)
	kwargs["a"] = Int(1)
	kwargs["b"] = String("hi")
	kwargs["c"] = list
	dict1 := Dict(kwargs)
	verifyMarshalUnMarshal(t, dict1)
	verifyMarshalUnMarshal(t, Dict(map[string]Value{}))
}

func TestUnequalMarshal(t *testing.T) {
	i := Int(2)
	d := Double(3.0)
	b := Bool(false)
	s := String("hi")
	l := List([]Value{Int(1), Double(2.0), Bool(true)})
	di := Dict(map[string]Value{"a": Int(2), "b": Double(1.0)})
	n := Nil
	verifyUnequalMarshal(t, Int(123), []Value{i, d, b, s, l, di, n})
	verifyUnequalMarshal(t, Double(-5.0), []Value{i, d, b, s, l, di, n})
	verifyUnequalMarshal(t, String("bye"), []Value{i, d, b, s, l, di, n})
	verifyUnequalMarshal(t, Bool(true), []Value{i, d, b, s, l, di, n})
	verifyUnequalMarshal(t, Nil, []Value{i, d, b, s, l, di})
	verifyUnequalMarshal(t, List([]Value{Int(2), Bool(true)}), []Value{i, d, b, s, l, di})
	verifyUnequalMarshal(t, Dict(map[string]Value{"b": Int(2)}), []Value{i, d, b, s, l, di})
}
