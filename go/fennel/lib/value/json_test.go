package value

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSON(t *testing.T) {
	type test struct {
		str string
		val Value
	}
	tests := []test{
		{str: "null", val: Nil},
		{str: "true", val: Bool(true)},
		{str: "false", val: Bool(false)},
		{str: "4", val: Int(4)},
		{str: "-3", val: Int(-3)},
		{str: "1.0", val: Double(1.0)},
		{str: "3.14", val: Double(3.14)},
		{str: "-111.11", val: Double(-111.11)},
		{str: `""`, val: String("")},
		{str: `"abc"`, val: String("abc")},
	}

	// Test nil pointer list and dict not marshalling to null
	// tests = append(tests, test{str: "{}", val: Dict(nil)})
	// tests = append(tests, test{str: "[[[],{}]]", val: List{List{List(nil), Dict(nil)}}})
	// tests = append(tests, test{str: `{"1":{"2":{"3":[],"4":{}}}}`, val: Dict{"1": Dict{"2": Dict{
	//	"3": List(nil), "4": Dict(nil),
	// }}}})

	l1 := NewList(Nil, Int(4), Double(3.14), String("xyz"))
	l1Str := `[null,4,3.14,"xyz"]`
	tests = append(tests, test{str: l1Str, val: l1})

	d1 := NewDict(map[string]Value{"k1": Double(3.14), "k2": Int(128), "k3": String("abc")})
	d1Str := `{"k1":3.14,"k2":128,"k3":"abc"}`
	tests = append(tests, test{str: d1Str, val: d1})

	l2 := NewList(Double(5.4), l1.Clone(), d1.Clone(), Nil)
	l2Str := fmt.Sprintf(`[5.4,%s,%s,null]`, l1Str, d1Str)
	tests = append(tests, test{str: l2Str, val: l2})

	d2 := NewDict(map[string]Value{"k1": Nil, "k2": l1, "k3": d1, "k4": l2})
	d2Str := fmt.Sprintf(`{"k1":null,"k2":%s,"k3":%s,"k4":%s}`, l1Str, d1Str, l2Str)
	tests = append(tests, test{str: d2Str, val: d2})

	// Test FromJSON()
	for _, tst := range tests {
		val, err := FromJSON([]byte(tst.str))
		assert.NoError(t, err, tst.str)
		assert.True(t, val.Equal(tst.val), tst.str)
	}
	// Test ToJSON()
	for _, tst := range tests {
		ser := ToJSON(tst.val)
		assert.Equal(t, tst.str, string(ser))
	}
}

func TestInvalidDict(t *testing.T) {
	dStr := `{1:3.14,"k2":128,"k3":"abc"}`
	_, err := FromJSON([]byte(dStr))
	assert.Error(t, err)
}
