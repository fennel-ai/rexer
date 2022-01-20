package data

import (
	"fennel/lib/utils"
	"fennel/test"
	"fennel/value"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDBBasic(t *testing.T) {
	DB, err := test.DefaultDB()
	assert.NoError(t, err)
	table, err := NewProfileTable(DB)
	assert.NoError(t, err)
	testProviderBasic(t, table)
}

func TestDBVersion(t *testing.T) {
	DB, err := test.DefaultDB()
	assert.NoError(t, err)
	table, err := NewProfileTable(DB)
	assert.NoError(t, err)
	testProviderVersion(t, table)
}

func TestLongKey(t *testing.T) {
	DB, err := test.DefaultDB()
	assert.NoError(t, err)
	table, err := NewProfileTable(DB)
	assert.NoError(t, err)
	err = table.Init()
	assert.NoError(t, err)

	val := value.Int(2)
	expected, _ := val.MarshalJSON()

	// can not set value on a key that is greater than 256 chars
	err = table.Set(1, 1232, utils.RandString(257), 1, expected)
	assert.Error(t, err)

	// but works for a key of size upto 256
	err = table.Set(1, 1232, utils.RandString(256), 1, expected)
	assert.NoError(t, err)
}
