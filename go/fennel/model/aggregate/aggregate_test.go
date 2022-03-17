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
	"google.golang.org/protobuf/proto"
)

func TestRetrieveStore(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	query := ast.Atom{Type: ast.Int, Lexeme: "4"}
	querySer, err := ast.Marshal(query)
	assert.NoError(t, err)

	options := aggregate.AggOptions{
		AggType:   "rolling_counter",
		Durations: []uint64{3600 * 24 * 7},
	}
	optionSer, err := proto.Marshal(&options)
	assert.NoError(t, err)
	agg := aggregate.AggregateSer{
		Name:      "test_counter",
		QuerySer:  querySer,
		Timestamp: 1,
		OptionSer: optionSer,
		Active:    true,
	}
	ctx := context.Background()

	// initially we can't retrieve
	found, err := Retrieve(ctx, tier, agg.Name)
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// store and retrieve again
	err = Store(ctx, tier, agg.Name, agg.QuerySer, agg.Timestamp, agg.OptionSer)
	assert.NoError(t, err)
	found, err = Retrieve(ctx, tier, agg.Name)
	assert.NoError(t, err)
	assert.Equal(t, agg, found)

	// and still can't retrieve if specs are different
	found, err = Retrieve(ctx, tier, "random agg name")
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// finally, storing for same name doesn't work
	query2 := ast.Atom{Type: ast.Int, Lexeme: "7"}
	querySer2, err := ast.Marshal(query2)
	assert.NoError(t, err)
	err = Store(ctx, tier, agg.Name, querySer2, agg.Timestamp+1, agg.OptionSer)
	assert.Error(t, err)
}

func TestRetrieveAll(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	options := aggregate.AggOptions{
		AggType: "rolling_counter",
	}
	optionSer, err := proto.Marshal(&options)
	assert.NoError(t, err)
	ctx := context.Background()

	agg := aggregate.AggregateSer{
		Timestamp: 1,
		OptionSer: optionSer,
		Active:    true,
	}
	expected := make([]aggregate.AggregateSer, 0)
	for i := 0; i < 5; i++ {
		found, err := RetrieveAll(ctx, tier)
		assert.NoError(t, err)
		assert.ElementsMatch(t, expected, found)
		agg.Name = ftypes.AggName(fmt.Sprintf("name:%d", i))
		agg.QuerySer = []byte(fmt.Sprintf("some query: %d", i))
		err = Store(ctx, tier, agg.Name, agg.QuerySer, agg.Timestamp, agg.OptionSer)
		assert.NoError(t, err)
		expected = append(expected, agg)
	}
}

func TestLongStrings(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	options := aggregate.AggOptions{
		AggType: "rolling_counter",
	}
	ctx := context.Background()
	optionSer, err := proto.Marshal(&options)
	assert.NoError(t, err)

	// can insert normal sized data
	err = Store(ctx, tier, "my_counter", []byte("query"), 1, optionSer)
	assert.NoError(t, err)

	// but can not if aggname is longer than 255 chars
	err = Store(ctx, tier, ftypes.AggName(utils.RandString(256)), []byte("query"), 1, optionSer)
	assert.Error(t, err)

	// but works if it is upto 255 chars
	err = Store(ctx, tier, ftypes.AggName(utils.RandString(255)), []byte("query"), 1, optionSer)
	assert.NoError(t, err)
}

func TestDeactivate(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	options := aggregate.AggOptions{
		AggType: "rolling_counter",
	}
	optionSer, err := proto.Marshal(&options)
	assert.NoError(t, err)

	err = Store(ctx, tier, "my_counter", []byte("query"), 1, optionSer)
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
}
