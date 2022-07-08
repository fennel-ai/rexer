//go:build glue

package aggregate

import (
	"context"
	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/test"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO(mohit): Remove build tag `glue` dependency for the following test cases
func TestDeactivateOffline(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	ctx := context.Background()
	aggOffline := aggregate.Aggregate{
		Name:      "my_counter_offline",
		Query:     ast.MakeInt(4),
		Timestamp: 1,
		Options: aggregate.Options{
			AggType:      "cf",
			Durations:    []uint32{3600 * 24 * 7},
			CronSchedule: "37 */2 * * ?",
			Limit:        10,
		},
	}

	err := Deactivate(ctx, tier, "my_counter_offline")
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
	assert.ErrorIs(t, err, aggregate.ErrNotActive)

	// And can deactivate multiple times
	err = Deactivate(ctx, tier, "my_counter_offline")
	assert.NoError(t, err)
}
