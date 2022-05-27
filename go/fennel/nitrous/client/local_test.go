package client

import (
	"fennel/test"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLocalClient(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	map_ := make(map[string]map[string]string)
	c := LocalConfig{
		tierID: tier.ID,
		Map:    &map_,
	}
	resource, err := c.Materialize()
	assert.NoError(t, err)
	client, ok := resource.(localClient)
	assert.True(t, ok)
	testClient(t, tier, client)
}
