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
)

func TestRetrieveAll(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	agg := aggregate.Aggregate{
		Timestamp: 1,
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint64{3600 * 24, 3600 * 24 * 2},
		},
	}
	// initially retrieve all is empty
	found, err := RetrieveAll(ctx, tier)
	assert.NoError(t, err)
	assert.Empty(t, found)

	expected := make([]aggregate.Aggregate, 0)
	for i := 0; i < 2; i++ {
		agg.Name = ftypes.AggName(fmt.Sprintf("name:%d", i))
		agg.Query = ast.MakeInt(int32(i))
		err = Store(ctx, tier, agg)
		assert.NoError(t, err)
		expected = append(expected, agg)
		found, err = RetrieveAll(ctx, tier)
		assert.NoError(t, err)
		assert.Equal(t, len(expected), len(found))
		for j, ag1 := range found {
			assert.True(t, expected[j].Equals(ag1))
		}
	}
}

func TestDuplicate(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	agg := aggregate.Aggregate{
		Name:      "test_counter",
		Query:     ast.Query{},
		Timestamp: 1,
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint64{3600 * 24 * 7},
		},
	}
	err = Store(ctx, tier, agg)
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
	agg.Query = ast.Query{}

	// Error if different options
	agg.Options.Durations = []uint64{3600 * 24 * 6}
	err = Store(ctx, tier, agg)
	assert.Error(t, err)
}

func TestDeactivate(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	agg := aggregate.Aggregate{
		Name:      "my_counter",
		Query:     ast.MakeInt(4),
		Timestamp: 1,
		Options: aggregate.Options{
			AggType:   "sum",
			Durations: []uint64{3600 * 24 * 7},
		},
	}

	// Deactivating when aggregate doesn't exist should throw an error
	err = Deactivate(ctx, tier, "my_counter")
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	err = Store(ctx, tier, agg)
	assert.NoError(t, err)

	// Can retrieve before deactivating
	_, err = Retrieve(ctx, tier, "my_counter")
	assert.NoError(t, err)

	err = Deactivate(ctx, tier, "my_counter")
	assert.NoError(t, err)

	// But cannot after deactivating
	_, err = Retrieve(ctx, tier, "my_counter")
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// And can deactivate multiple times
	err = Deactivate(ctx, tier, "my_counter")
	assert.NoError(t, err)

}

func TestDeactivateOffline(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	aggOffline := aggregate.Aggregate{
		Name:      "my_counter_offline",
		Query:     ast.MakeInt(4),
		Timestamp: 1,
		Options: aggregate.Options{
			AggType:      "topk",
			Durations:    []uint64{3600 * 24 * 7},
			CronSchedule: "20 1 * * ?",
		},
	}

	err = Deactivate(ctx, tier, "my_counter_offline")
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	err = Store(ctx, tier, aggOffline)
	assert.NoError(t, err)

	// Can retrieve before deactivating
	_, err = Retrieve(ctx, tier, "my_counter_offline")
	assert.NoError(t, err)

	err = Deactivate(ctx, tier, "my_counter_offline")
	assert.NoError(t, err)

	// But cannot after deactivating
	_, err = Retrieve(ctx, tier, "my_counter_offline")
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// And can deactivate multiple times
	err = Deactivate(ctx, tier, "my_counter_offline")
	assert.NoError(t, err)
}
