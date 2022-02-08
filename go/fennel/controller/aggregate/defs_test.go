package aggregate

import (
	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/test"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetrieveAll(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	agg := aggregate.Aggregate{
		Timestamp: 1,
		Options: aggregate.AggOptions{
			AggType:  "rolling_counter",
			Duration: 3600 * 24,
		},
	}
	// initially retrieve all is empty
	found, err := RetrieveAll(tier)
	assert.NoError(t, err)
	assert.Empty(t, found)

	expected := make([]aggregate.Aggregate, 0)
	for i := 0; i < 2; i++ {
		agg.Name = ftypes.AggName(fmt.Sprintf("name:%d", i))
		agg.Query = ast.MakeInt(int32(i))
		err = Store(tier, agg)
		assert.NoError(t, err)
		expected = append(expected, agg)
		found, err = RetrieveAll(tier)
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

	agg := aggregate.Aggregate{
		Name:      "test_counter",
		Query:     ast.MakeInt(4),
		Timestamp: 1,
		Options: aggregate.AggOptions{
			AggType:  "rolling_counter",
			Duration: uint64(time.Hour * 24 * 7),
		},
	}
	err = Store(tier, agg)
	assert.NoError(t, err)

	// No error with duplicate store with different timestamp
	agg.Timestamp = 2
	err = Store(tier, agg)
	assert.NoError(t, err)

	// Error if different query
	agg.Query = ast.MakeInt(6)
	err = Store(tier, agg)
	assert.Error(t, err)
	agg.Query = ast.MakeInt(4)

	// Error if different options
	agg.Options.Duration = uint64(time.Hour * 24 * 6)
	err = Store(tier, agg)
	assert.Error(t, err)
}

func TestDeactivate(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	agg := aggregate.Aggregate{
		Name:      "my_counter",
		Query:     ast.MakeInt(4),
		Timestamp: 1,
		Options: aggregate.AggOptions{
			AggType:  "rolling_counter",
			Duration: uint64(time.Hour * 24 * 7),
		},
	}

	err = Store(tier, agg)
	assert.NoError(t, err)

	// Can retrieve before deactivating
	_, err = Retrieve(tier, "my_counter")
	assert.NoError(t, err)

	err = Deactivate(tier, "my_counter")
	assert.NoError(t, err)

	// But cannot after deactivating
	_, err = Retrieve(tier, "my_counter")
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)
}
