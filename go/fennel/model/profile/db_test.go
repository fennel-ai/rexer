package profile

import (
	"fennel/lib/utils"
	"fennel/test"
	"fennel/value"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDBBasic(t *testing.T) {
	testProviderBasic(t, dbProvider{})
}

func TestDBVersion(t *testing.T) {
	testProviderVersion(t, dbProvider{})
}

func TestLongKey(t *testing.T) {
	this, err := test.DefaultInstance()
	assert.NoError(t, err)
	p := dbProvider{}

	val := value.Int(2)
	expected, _ := val.MarshalJSON()

	// can not set value on a makeKey that is greater than 256 chars
	err = p.set(this, 1, 1232, utils.RandString(257), 1, expected)
	assert.Error(t, err)

	// but works for a makeKey of size upto 256
	err = p.set(this, 1, 1232, utils.RandString(256), 1, expected)
	assert.NoError(t, err)
}
