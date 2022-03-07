package eks

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

	_, err = Insert(m, lib.EKS{
		ClusterID:    "id",
		MinInstances: 1,
		MaxInstances: 2,
		InstanceType: "type",
	})
	assert.NoError(t, err)
}
