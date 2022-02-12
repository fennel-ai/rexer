package value

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
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
		{str: "3.14", val: Double(3.14)},
		{str: "-111.11", val: Double(-111.11)},
		{str: `""`, val: String("")},
		{str: `"abc"`, val: String("abc")},
	}

	l1 := List{Nil, Int(4), Double(3.14), String("xyz")}
	l1Str := "[null,4,3.14,\"xyz\"]"
	tests = append(tests, test{str: l1Str, val: l1})

	d1 := Dict{"k1": Double(3.14), "k2": Int(128), "k3": String("abc")}
	d1Str := "{\"k1\":3.14,\"k2\":128,\"k3\":\"abc\"}"
	tests = append(tests, test{str: d1Str, val: d1})

	l2 := List{Double(5.4), l1.Clone(), d1.Clone(), Nil}
	l2Str := fmt.Sprintf("[5.4,%s,%s,null]", l1Str, d1Str)
	tests = append(tests, test{str: l2Str, val: l2})

	d2 := Dict{"k1": Nil, "k2": l1, "k3": d1, "k4": l2}
	d2Str := fmt.Sprintf("{\"k1\":null,\"k2\":%s,\"k3\":%s,\"k4\":%s}", l1Str, d1Str, l2Str)
	tests = append(tests, test{str: d2Str, val: d2})

	// Test FromJSON()
	for _, tst := range tests {
		val, err := FromJSON([]byte(tst.str))
		assert.NoError(t, err)
		assert.True(t, tst.val.Equal(val))
	}
	// Test ToJSON()
	for _, tst := range tests {
		jsonbytes, err := ToJSON(tst.val)
		assert.NoError(t, err)
		assert.Equal(t, tst.str, string(jsonbytes))
	}
}

func TestInvalidDict(t *testing.T) {
	dStr := "{1:3.14,\"k2\":128,\"k3\":\"abc\"}"
	_, err := FromJSON([]byte(dStr))
	assert.Error(t, err)
}
