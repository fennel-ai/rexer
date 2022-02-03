package profile

import (
	"testing"

	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestDBBasic(t *testing.T) {
	testProviderBasic(t, dbProvider{})
}

func TestDBVersion(t *testing.T) {
	testProviderVersion(t, dbProvider{})
}

func TestLongKey(t *testing.T) {
	this, err := test.MockPlane()
	assert.NoError(t, err)
	p := dbProvider{}

	val := value.Int(2)
	expected, _ := value.Marshal(val)

	// can not set value on a makeKey that is greater than 256 chars
	err = p.set(this, 1, "1", 1232, utils.RandString(257), 1, expected)
	assert.Error(t, err)

	// but works for a makeKey of size upto 256
	err = p.set(this, 1, "1", 1232, utils.RandString(256), 1, expected)
	assert.NoError(t, err)
}

func TestLongOType(t *testing.T) {
	this, err := test.MockPlane()
	assert.NoError(t, err)
	p := dbProvider{}

	val := value.Int(5)
	expected, _ := value.Marshal(val)

	// otype cannot be longer than 256 chars
	err = p.set(this, 1, ftypes.OType(utils.RandString(257)), 23, "key", 1, expected)
	assert.Error(t, err)

	// but works for otype of length 256 chars
	err = p.set(this, 1, ftypes.OType(utils.RandString(256)), 23, "key", 1, expected)
	assert.NoError(t, err)
}
