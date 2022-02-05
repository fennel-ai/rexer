package query

import (
	"fennel/lib/ftypes"
	"fennel/lib/query"
	"fennel/test"
	"fennel/tier"
	"testing"

	"github.com/stretchr/testify/assert"
)

func verifyGet(t *testing.T, tier tier.Tier, request query.QueryRequest, expected []query.QuerySer) {
	queries, err := Get(tier, request)
	assert.NoError(t, err)
	assert.Equal(t, expected, queries)
}

func TestInsertGet(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	// initially no queries even with no filters
	queries, err := Get(tier, query.QueryRequest{})
	assert.NoError(t, err)
	assert.Empty(t, queries)

	// set a couple of queries and verify we can get them
	ts1 := ftypes.Timestamp(1)
	query1 := query.QuerySer{QueryId: 0, Custid: tier.CustID, Timestamp: ts1, QuerySer: "hello"}

	queryID1, err := Insert(tier, query1.Custid, query1.Timestamp, query1.QuerySer)
	assert.NoError(t, err)
	query1.QueryId = queryID1

	verifyGet(t, tier, query.QueryRequest{}, []query.QuerySer{query1})
	verifyGet(t, tier, query.QueryRequest{QueryId: queryID1}, []query.QuerySer{query1})
	verifyGet(t, tier, query.QueryRequest{QueryId: queryID1}, []query.QuerySer{query1})
	verifyGet(t, tier, query.QueryRequest{Custid: tier.CustID}, []query.QuerySer{query1})
	verifyGet(t, tier, query.QueryRequest{Custid: tier.CustID, MinTimestamp: ts1 - 1}, []query.QuerySer{query1})
	verifyGet(t, tier, query.QueryRequest{Custid: tier.CustID, MinTimestamp: ts1}, []query.QuerySer{query1})
	verifyGet(t, tier, query.QueryRequest{Custid: tier.CustID, MinTimestamp: ts1 + 1}, []query.QuerySer{})

	ts2 := ftypes.Timestamp(3)
	query2 := query.QuerySer{QueryId: 0, Custid: tier.CustID, Timestamp: ts2, QuerySer: "bye"}
	queryID2, err := Insert(tier, query2.Custid, query2.Timestamp, query2.QuerySer)
	assert.NoError(t, err)
	query2.QueryId = queryID2

	verifyGet(t, tier, query.QueryRequest{}, []query.QuerySer{query1, query2})
	verifyGet(t, tier, query.QueryRequest{QueryId: queryID1}, []query.QuerySer{query1})
	verifyGet(t, tier, query.QueryRequest{QueryId: queryID2}, []query.QuerySer{query2})
	verifyGet(t, tier, query.QueryRequest{Custid: tier.CustID}, []query.QuerySer{query1, query2})
	verifyGet(t, tier, query.QueryRequest{Custid: tier.CustID, MinTimestamp: ts1 - 1}, []query.QuerySer{query1, query2})
	verifyGet(t, tier, query.QueryRequest{Custid: tier.CustID, MinTimestamp: ts1}, []query.QuerySer{query1, query2})
	verifyGet(t, tier, query.QueryRequest{Custid: tier.CustID, MinTimestamp: ts1 + 1}, []query.QuerySer{query2})
	verifyGet(t, tier, query.QueryRequest{Custid: tier.CustID, MaxTimestamp: ts1}, []query.QuerySer{})
	verifyGet(t, tier, query.QueryRequest{Custid: tier.CustID, MaxTimestamp: ts2}, []query.QuerySer{query1})
}
