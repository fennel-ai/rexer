package query

import (
	"context"
	"testing"

	"fennel/engine/ast"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestQuery(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	// trying to get a query not in DB should fail
	_, err := Get(ctx, tier, "query")
	assert.Error(t, err)

	// store query now
	tree := ast.MakeInt(5)
	qID, err := Insert(ctx, tier, "name", tree, "description")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), qID)

	qID2, err := Insert(ctx, tier, "name", tree, "description")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), qID2)

	_, err = Insert(ctx, tier, "name", ast.MakeInt(6), "description")
	assert.Error(t, err)

	_, err = Insert(ctx, tier, "name", tree, "description2")
	assert.Error(t, err)

	// check get works
	found, err := Get(ctx, tier, "name")
	assert.NoError(t, err)
	assert.Equal(t, tree, found)
}
