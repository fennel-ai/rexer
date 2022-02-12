package value

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

var JsonTestStrings [][]byte
var JsonTestValues []Value

func TestJson(t *testing.T) {
	addJsonTests(t)

	// Test FromJson()
	for i, v := range JsonTestValues {
		v2, err := FromJson(JsonTestStrings[i])
		assert.NoError(t, err)
		assert.True(t, v.Equal(v2))
	}

	// Test ToJson()
	for i, v := range JsonTestValues {
		jsonstr, err := ToJson(v)
		assert.NoError(t, err)
		assert.Equal(t, JsonTestStrings[i], jsonstr)
	}
}

func TestInvalidDict(t *testing.T) {
	dJson := "{1:3.14,\"k2\":128,\"k3\":\"abc\"}"

	_, err := FromJson([]byte(dJson))
	assert.Error(t, err)
}

func addJsonTests(t *testing.T) {
	addJsonTest(Nil, "null")
	addJsonTest(Bool(true), "true")
	addJsonTest(Bool(false), "false")
	addJsonTest(Int(4), "4")
	addJsonTest(Int(-3), "-3")
	addJsonTest(Double(3.14), "3.14")
	addJsonTest(Double(-111.11), "-111.11")
	addJsonTest(String(""), "\"\"")
	addJsonTest(String("abc"), "\"abc\"")

	l1 := List{Nil, Int(4), Double(3.14), String("xyz")}
	l1Json := "[null,4,3.14,\"xyz\"]"
	addJsonTest(l1, l1Json)

	d1 := Dict{"k1": Double(3.14), "k2": Int(128), "k3": String("abc")}
	d1Json := "{\"k1\":3.14,\"k2\":128,\"k3\":\"abc\"}"
	addJsonTest(d1, d1Json)

	l2 := List{Double(5.4), l1.Clone(), d1.Clone(), Nil}
	l2Json := fmt.Sprintf("[5.4,%s,%s,null]", l1Json, d1Json)
	addJsonTest(l2, l2Json)

	d2 := Dict{"k1": Nil, "k2": l1, "k3": d1, "k4": l2}
	d2Json := fmt.Sprintf("{\"k1\":null,\"k2\":%s,\"k3\":%s,\"k4\":%s}", l1Json, d1Json, l2Json)
	addJsonTest(d2, d2Json)
}

func addJsonTest(val Value, s string) {
	JsonTestValues = append(JsonTestValues, val)
	JsonTestStrings = append(JsonTestStrings, []byte(s))
}
