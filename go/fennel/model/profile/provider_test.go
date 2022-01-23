package profile

import (
	"fennel/lib/value"
	"fennel/test"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testProviderBasic(t *testing.T, p provider) {
	this, err := test.DefaultInstance()
	assert.NoError(t, err)
	val := value.Int(2)
	expected, _ := value.Marshal(val)

	// initially before setting, value isn't there so we get nil back
	found, err := p.get(this, 1, 1232, "summary", 1)
	// and calling get on a row that doesn't exist is not an error
	assert.NoError(t, err)
	assert.Equal(t, []byte(nil), found)

	// now set the value
	err = p.set(this, 1, 1232, "summary", 1, expected)
	assert.NoError(t, err)

	// now get the same value back
	found, err = p.get(this, 1, 1232, "summary", 1)
	assert.Equal(t, expected, found)

	// and get it again to verify nothing changes
	found, err = p.get(this, 1, 1232, "summary", 1)
	assert.Equal(t, expected, found)
}

func testProviderVersion(t *testing.T, p provider) {
	this, err := test.DefaultInstance()
	assert.NoError(t, err)

	val1 := value.Int(2)
	expected1, _ := value.Marshal(val1)

	// first setting a version of 0 isn't possible
	err = p.set(this, 1, 1232, "summary", 0, expected1)
	assert.Error(t, err)

	// but it works with a valid version
	err = p.set(this, 1, 1232, "summary", 1, expected1)
	assert.NoError(t, err)

	// and can set another version on the same value
	val2 := value.String("hello")
	expected2, _ := value.Marshal(val2)
	err = p.set(this, 1, 1232, "summary", 2, expected2)
	assert.NoError(t, err)

	// versions can also be non-continuous
	val3 := value.Dict(map[string]value.Value{
		"hi":  value.Int(1),
		"bye": value.List([]value.Value{value.Bool(true), value.String("yo")}),
	})
	expected3, _ := value.Marshal(val3)
	err = p.set(this, 1, 1232, "summary", 10, expected3)
	assert.NoError(t, err)

	// we can get any of these versions back
	found, err := p.get(this, 1, 1232, "summary", 1)
	assert.NoError(t, err)
	assert.Equal(t, expected1, found)

	found, err = p.get(this, 1, 1232, "summary", 2)
	assert.NoError(t, err)
	assert.Equal(t, expected2, found)

	found, err = p.get(this, 1, 1232, "summary", 10)
	assert.NoError(t, err)
	assert.Equal(t, expected3, found)

	// if we ask for version 0, by default get the highest version
	found, err = p.get(this, 1, 1232, "summary", 0)
	assert.NoError(t, err)
	assert.Equal(t, expected3, found)

	// and asking for a version that doesn't exist return empty string
	found, err = p.get(this, 1, 1232, "summary", 5)
	assert.NoError(t, err)
	assert.Equal(t, []byte(nil), found)
}
