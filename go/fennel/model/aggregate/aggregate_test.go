package aggregate

import (
	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/test"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
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
	assert.ErrorIs(t, err, ErrNotFound)

	// store and retrieve again
	err = Store(instance, agg.Type, agg.Name, agg.QuerySer, agg.Timestamp, agg.OptionSer)
	assert.NoError(t, err)
	found, err = Retrieve(instance, agg.Type, agg.Name)
	assert.NoError(t, err)
	assert.Equal(t, agg, found)

	// and still can't retrieve if specs are different
	found, err = Retrieve(instance, "random agg type", agg.Name)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound)

	found, err = Retrieve(instance, agg.Type, "random agg name")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound)
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
