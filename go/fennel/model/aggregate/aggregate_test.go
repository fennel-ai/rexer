package aggregate

import (
	"fmt"
	"testing"
	"time"

	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/test"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestRetrieveStore(t *testing.T) {
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)

	query := ast.Atom{Type: ast.Int, Lexeme: "4"}
	querySer, err := ast.Marshal(query)
	assert.NoError(t, err)

	options := aggregate.AggOptions{
		WindowType: aggregate.WindowType_LAST,
		Duration:   uint64(time.Hour * 24 * 7),
		Retention:  0,
	}
	optionSer, err := proto.Marshal(&options)
	assert.NoError(t, err)
	agg := aggregate.AggregateSer{
		CustID:    instance.CustID,
		Type:      "counter",
		Name:      "test_counter",
		QuerySer:  querySer,
		Timestamp: 1,
		OptionSer: optionSer,
	}

	// initially we can't retrieve
	found, err := Retrieve(instance, agg.Type, agg.Name)
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// store and retrieve again
	err = Store(instance, agg.Type, agg.Name, agg.QuerySer, agg.Timestamp, agg.OptionSer)
	assert.NoError(t, err)
	found, err = Retrieve(instance, agg.Type, agg.Name)
	assert.NoError(t, err)
	assert.Equal(t, agg, found)

	// and still can't retrieve if specs are different
	found, err = Retrieve(instance, "random agg type", agg.Name)
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	found, err = Retrieve(instance, agg.Type, "random agg name")
	assert.Error(t, err)
	assert.ErrorIs(t, err, aggregate.ErrNotFound)

	// finally, storing for same type/name doesn't work
	query2 := ast.Atom{Type: ast.Int, Lexeme: "7"}
	querySer2, err := ast.Marshal(query2)
	assert.NoError(t, err)
	err = Store(instance, agg.Type, agg.Name, querySer2, agg.Timestamp+1, agg.OptionSer)
	assert.Error(t, err)
}

func TestRetrieveAll(t *testing.T) {
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)

	agg := aggregate.AggregateSer{
		CustID:    instance.CustID,
		Type:      "counter",
		Timestamp: 1,
		OptionSer: []byte("some options"),
	}
	expected := make([]aggregate.AggregateSer, 0)
	for i := 0; i < 5; i++ {
		found, err := RetrieveAll(instance, agg.Type)
		assert.NoError(t, err)
		assert.Equal(t, expected, found)
		agg.Name = ftypes.AggName(fmt.Sprintf("name:%d", i))
		agg.QuerySer = []byte(fmt.Sprintf("some query: %d", i))
		err = Store(instance, agg.Type, agg.Name, agg.QuerySer, agg.Timestamp, agg.OptionSer)
		assert.NoError(t, err)
		expected = append(expected, agg)
	}
}

func TestLongStrings(t *testing.T) {
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)

	// can not insert normal sized data
	err = Store(instance, "counter", "my_counter", []byte("query"), 1, []byte("some options"))
	assert.NoError(t, err)

	// but can not if either aggtype or name is longer than 255 chars
	err = Store(instance, ftypes.AggType(utils.RandString(256)), "my_counter", []byte("query"), 1, []byte("some options"))
	assert.Error(t, err)
	err = Store(instance, "counter", ftypes.AggName(utils.RandString(256)), []byte("query"), 1, []byte("some options"))
	assert.Error(t, err)

	// but works if it is upto 255 chars
	err = Store(instance, ftypes.AggType(utils.RandString(255)), ftypes.AggName(utils.RandString(255)), []byte("query"), 1, []byte("some options"))
	assert.NoError(t, err)
}
