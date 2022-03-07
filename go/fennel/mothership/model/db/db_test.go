package db

import (
	"testing"

	"fennel/mothership"
	"fennel/mothership/lib"
	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer mothership.Teardown(m)

	_, err = Insert(m, lib.DB{
		ClusterID:            "id",
		ClusterSecurityGroup: "group",
		DBHost:               "hostname",
		AdminUsername:        "username",
		AdminPassword:        "password",
	})
	assert.NoError(t, err)
}
