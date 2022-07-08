package query

import (
	"testing"

	"fennel/engine/ast"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	// trying to get a query not in DB should fail
	_, err := Get(tier, "query")
	assert.Error(t, err)

	// store query now
	tree := ast.MakeInt(5)
	qID, err := Insert(tier, "name", tree)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), qID)

	// check get works
	found, err := Get(tier, "name")
	assert.NoError(t, err)
	assert.Equal(t, tree, found)
}
