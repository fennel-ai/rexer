package aggregate

import (
	"context"
	"fmt"
	"testing"

	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetrieveActive(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	ctx := context.Background()
	agg := aggregate.Aggregate{
		Timestamp: 1,
		Mode:      "rql",
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint32{3600 * 24, 3600 * 24 * 2},
		},
	}
	// initially retrieve all is empty
	found, err := RetrieveActive(ctx, tier)
	assert.NoError(t, err)
	assert.Empty(t, found)

	expected := make([]aggregate.Aggregate, 0)
	for i := 0; i < 2; i++ {
		agg.Name = ftypes.AggName(fmt.Sprintf("name:%d", i))
		agg.Query = ast.MakeInt(int32(i))
		agg.Id = ftypes.AggId(i)
		err = Store(ctx, tier, agg)
		assert.NoError(t, err)
		expected = append(expected, agg)
		found, err = RetrieveActive(ctx, tier)
		assert.NoError(t, err)
		assert.Equal(t, len(expected), len(found))
		for j, ag1 := range found {
			assert.True(t, expected[j].Equals(ag1))
		}
	}
}

func TestDuplicate(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	ctx := context.Background()
	agg := aggregate.Aggregate{
		Name:      "test_counter",
		Query:     &ast.Query{},
		Mode:      "rql",
		Timestamp: 1,
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint32{3600 * 24 * 7},
		},
	}
	err := Store(ctx, tier, agg)
	assert.NoError(t, err)

	// No error with duplicate store with different timestamp
	// Naive equality comparison would panic here as
	// ast Query contains slice which is incomparable
	agg.Timestamp = 2
	err = Store(ctx, tier, agg)
	assert.NoError(t, err)

	// Error if different query
	agg.Query = ast.MakeInt(4)
	err = Store(ctx, tier, agg)
	assert.Error(t, err)
	agg.Query = &ast.Query{}

	// Error if different options
	agg.Options.Durations = []uint32{3600 * 24 * 6}
	err = Store(ctx, tier, agg)
	assert.Error(t, err)
}

func TestDeactivate(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	ctx := context.Background()
	agg := aggregate.Aggregate{
		Name:      "my_counter",
		Query:     ast.MakeInt(4),
		Timestamp: 1,
		Mode:      "rql",
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint32{3600 * 24 * 7},
		},
	}

	// Deactivating when aggregate doesn't exist should throw an error
	err := Deactivate(ctx, tier, "my_counter")
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// Can retrieve before deactivating
	err = Store(ctx, tier, agg)
	assert.NoError(t, err)

	_, err = Retrieve(ctx, tier, "my_counter")
	assert.NoError(t, err)

	// Retrieve after deactivating should return ErrNotActive.
	err = Deactivate(ctx, tier, "my_counter")
	assert.NoError(t, err)
	_, err = Retrieve(ctx, tier, "my_counter")
	assert.ErrorIs(t, err, aggregate.ErrNotActive)

	// Can deactivate multiple times
	err = Deactivate(ctx, tier, "my_counter")
	assert.NoError(t, err)
}

func TestReactivate(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	ctx := context.Background()
	agg := aggregate.Aggregate{
		Name:      "my-aggregate",
		Timestamp: 1,
		Mode:      "rql",
		Query:     ast.MakeInt(1),
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint32{3600 * 24, 3600 * 24 * 2},
		},
	}

	// initially retrieve all is empty
	_, err := Retrieve(ctx, tier, agg.Name)
	require.ErrorIs(t, err, aggregate.ErrNotFound)

	err = Store(ctx, tier, agg)
	require.NoError(t, err)
	agg.Id = 1
	agg.Active = true

	got, err := Retrieve(ctx, tier, agg.Name)
	require.NoError(t, err)
	require.Equal(t, agg, got)

	err = Deactivate(ctx, tier, agg.Name)
	require.NoError(t, err)

	// Remove aggregate definition from cache.
	tier.AggregateDefs.Delete(agg.Name)

	err = Store(ctx, tier, agg)
	require.NoError(t, err)

	got, err = Retrieve(ctx, tier, agg.Name)
	require.NoError(t, err)
	require.Equal(t, agg, got)
}
