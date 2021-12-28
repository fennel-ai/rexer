package main

import (
	"fennel/profile/client"
	_ "fennel/profile/client"
	"fennel/profile/lib"
	"fennel/value"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TODO: add more tests covering more error conditions
func TestServerClientBasic(t *testing.T) {
	// start the server
	go main()
	// and create a client
	c := client.NewClient(fmt.Sprintf("http://localhost:%d", PORT))

	// in the beginning, with no value set, we get nil pointer back but with no error
	checkGetSet(t, c, true, 1, 1, 0, "age", value.Value(nil))

	var expected value.Value = value.List([]value.Value{value.Int(1), value.Bool(false), value.Nil})
	checkGetSet(t, c, false, 1, 1, 1, "age", expected)

	// we can also get it without using the specific version number
	checkGetSet(t, c, true, 1, 1, 0, "age", expected)

	// set few more key/value pairs and verify it works
	checkGetSet(t, c, false, 1, 1, 2, "age", value.Nil)
	checkGetSet(t, c, false, 1, 3, 2, "age", value.Int(1))
	checkGetSet(t, c, true, 1, 1, 2, "age", value.Nil)
	checkGetSet(t, c, true, 1, 1, 0, "age", value.Nil)
	checkGetSet(t, c, false, 10, 3131, 0, "summary", value.Int(1))
}

func checkGetSet(t *testing.T, c client.Client, get bool, otype lib.OType, oid uint64, version uint64, key string, val value.Value) {
	if get {
		found, err := c.Get(otype, oid, key, version)
		assert.NoError(t, err)
		if found == nil {
			assert.Equal(t, nil, val)
		} else {
			assert.Equal(t, val, *found)
		}
	} else {
		err := c.Set(otype, oid, key, version, val)
		assert.NoError(t, err)
		found, err := c.Get(otype, oid, key, version)
		assert.NoError(t, err)
		if found == nil {
			assert.Equal(t, nil, val)
		} else {
			assert.Equal(t, val, *found)
		}
	}
}
