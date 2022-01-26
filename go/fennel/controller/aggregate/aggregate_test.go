package aggregate

import (
	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/test"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRetrieveAll(t *testing.T) {
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)

	// calling retrievall on invalid type returns an error
	_, err = RetrieveAll(instance, "some invalid type")
	assert.Error(t, err)

	agg := aggregate.Aggregate{
		CustID:    instance.CustID,
		Type:      "counter",
		Timestamp: 1,
		Options:   aggregate.AggOptions{},
	}
	// initially retrieve all is empty
	found, err := RetrieveAll(instance, agg.Type)
	assert.NoError(t, err)
	assert.Empty(t, found)

	expected := make([]aggregate.Aggregate, 0)
	for i := 0; i < 2; i++ {
		agg.Name = ftypes.AggName(fmt.Sprintf("name:%d", i))
		agg.Query = ast.MakeInt(int32(i))
		err = Store(instance, agg)
		assert.NoError(t, err)
		expected = append(expected, agg)
		found, err = RetrieveAll(instance, agg.Type)
		assert.NoError(t, err)
		assert.Equal(t, len(expected), len(found))
		for j, ag1 := range found {
			assert.True(t, expected[j].Equals(ag1))
		}
	}
}
