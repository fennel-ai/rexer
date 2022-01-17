package data

import (
	"fennel/db"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDBBasic(t *testing.T) {
	DB, err := db.Default()
	assert.NoError(t, err)
	table, err := NewProfileTable(DB.(db.Connection))
	assert.NoError(t, err)
	testProviderBasic(t, table)
}

func TestDBVersion(t *testing.T) {
	DB, err := db.Default()
	assert.NoError(t, err)
	table, err := NewProfileTable(DB.(db.Connection))
	assert.NoError(t, err)
	testProviderVersion(t, table)
}
