package data

import (
	"fennel/db"
	"fennel/instance"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDBBasic(t *testing.T) {
	err := instance.Setup([]instance.Resource{instance.DB})
	assert.NoError(t, err)
	p := DB{"profile", db.DB}
	testProviderBasic(t, p)
}

func TestDBVersion(t *testing.T) {
	err := instance.Setup([]instance.Resource{instance.DB})
	assert.NoError(t, err)
	p := DB{"profile", db.DB}
	testProviderVersion(t, p)
}
