package aggregate

import (
	"context"
	"fmt"
	"testing"

	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestRetrieveStore(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	agg := aggregate.Aggregate{
		Name:      "test_counter",
		Query:     &ast.Atom{Type: ast.Int, Lexeme: "4"},
		Timestamp: 1,
		Options: aggregate.Options{
			AggType:   "rolling_counter",
			Durations: []uint32{3600 * 24 * 7},
		},
		Active: true,
	}
	ctx := context.Background()

	// initially we can't retrieve
	_, err := Retrieve(ctx, tier, agg.Name)
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// store and retrieve again
	err = Store(ctx, tier, agg)
	// Id is set by the DB
	agg.Id = 1

	assert.NoError(t, err)
	found, err := Retrieve(ctx, tier, agg.Name)
	assert.NoError(t, err)
	assert.Equal(t, agg, found)

	// and still can't retrieve if specs are different
	_, err = Retrieve(ctx, tier, "random agg name")
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// finally, storing for same name doesn't work
	agg.Query = &ast.Atom{Type: ast.Int, Lexeme: "7"}
	err = Store(ctx, tier, agg)
	assert.Error(t, err)
}

func TestRetrieveActive(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	options := aggregate.Options{
		AggType: "rolling_counter",
	}
	ctx := context.Background()

	agg := aggregate.Aggregate{
		Timestamp: 1,
		Options:   options,
		Active:    true,
	}
	var expected []aggregate.Aggregate
	for i := 0; i < 5; i++ {
		found, err := RetrieveActive(ctx, tier)
		assert.NoError(t, err)
		assert.ElementsMatch(t, expected, found)
		agg.Name = ftypes.AggName(fmt.Sprintf("name:%d", i))
		agg.Query = ast.MakeString(fmt.Sprintf("some query: %d", i))
		err = Store(ctx, tier, agg)
		agg.Id = ftypes.AggId(i + 1)
		assert.NoError(t, err)
		expected = append(expected, agg)
	}
}

func TestRetrieveAll(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	options := aggregate.Options{
		AggType: "rolling_counter",
	}
	ctx := context.Background()

	var aggs []aggregate.Aggregate
	// store few aggregates with mixed Active status
	for i := 0; i < 5; i++ {
		agg := aggregate.Aggregate{
			Name:      ftypes.AggName(fmt.Sprintf("name:%d", i)),
			Timestamp: 1,
			Options:   options,
			Active:    true,
			Query:     ast.MakeString(fmt.Sprintf("some query: %d", i)),
		}
		err := Store(ctx, tier, agg)
		assert.NoError(t, err)
		agg.Id = ftypes.AggId(i + 1)
		aggs = append(aggs, agg)
	}

	actual, err := RetrieveAll(ctx, tier)
	assert.NoError(t, err)
	assert.Equal(t, actual, aggs)
}

func TestLongStrings(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	options := aggregate.Options{
		AggType: "rolling_counter",
	}
	ctx := context.Background()

	// can insert normal sized data
	agg := aggregate.Aggregate{
		Name:      "my_counter",
		Timestamp: 1,
		Options:   options,
		Active:    true,
		Query:     ast.MakeString("query"),
	}
	err := Store(ctx, tier, agg)
	assert.NoError(t, err)

	// but can not if aggname is longer than 255 chars
	agg.Name = ftypes.AggName(utils.RandString(256))
	err = Store(ctx, tier, agg)
	assert.Error(t, err)

	// but works if it is upto 255 chars
	agg.Name = ftypes.AggName(utils.RandString(255))
	err = Store(ctx, tier, agg)
	assert.NoError(t, err)
}

func TestDeactivate(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	ctx := context.Background()
	options := aggregate.Options{
		AggType: "rolling_counter",
	}
	agg := aggregate.Aggregate{
		Name:      "my_counter",
		Timestamp: 1,
		Options:   options,
		Active:    true,
		Query:     ast.MakeString("query"),
	}

	// Store and retrieve - active should be "true"
	err := Store(ctx, tier, agg)
	assert.NoError(t, err)
	got, err := Retrieve(ctx, tier, "my_counter")
	assert.NoError(t, err)
	assert.True(t, got.Active)

	// Deactivate and retrieve - active should be "false"
	err = Deactivate(ctx, tier, "my_counter")
	assert.NoError(t, err)
	got, err = Retrieve(ctx, tier, "my_counter")
	assert.NoError(t, err)
	assert.False(t, got.Active)

	// Reactivate and retrieve - active should be "true"
	err = Activate(ctx, tier, "my_counter")
	assert.NoError(t, err)
	got, err = Retrieve(ctx, tier, "my_counter")
	assert.NoError(t, err)
	assert.True(t, got.Active)
}
