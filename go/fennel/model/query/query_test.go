package query

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/lib/query"
	"fennel/test"
	"fennel/tier"
	"github.com/stretchr/testify/assert"
	"testing"
)

func verifyRetrieve(t *testing.T, tier tier.Tier, name string, expected query.QuerySer) {
	ctx := context.Background()
	query, err := Retrieve(ctx, tier, name)
	assert.NoError(t, err)
	assert.Equal(t, expected, query)
}

func TestInsertGet(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	// set a couple of queries and verify we can get them
	ts1 := ftypes.Timestamp(1)
	query1 := query.QuerySer{QueryId: 0, Name: "name", Timestamp: ts1, QuerySer: []byte("hello"), Description: "description"}

	queryID1, err := Insert(tier, query1.Name, query1.Timestamp, query1.QuerySer, "description")
	assert.NoError(t, err)
	query1.QueryId = queryID1

	verifyRetrieve(t, tier, "name", query1)
	verifyRetrieve(t, tier, query1.Name, query1)

	ts2 := ftypes.Timestamp(3)
	query2 := query.QuerySer{QueryId: 0, Name: "query2", Timestamp: ts2, QuerySer: []byte("bye"), Description: "description2"}
	queryID2, err := Insert(tier, query2.Name, query2.Timestamp, query2.QuerySer, "description2")
	assert.NoError(t, err)
	query2.QueryId = queryID2

	verifyRetrieve(t, tier, "query2", query2)
}
