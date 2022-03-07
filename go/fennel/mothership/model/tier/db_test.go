package tier

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

	_, err = Insert(m, lib.Tier{
		DataPlaneID:  1,
		CustomerID:   2,
		PulumiStack:  "pulumi",
		APIURL:       "url",
		K8sNamespace: "namespace",
	})

}
