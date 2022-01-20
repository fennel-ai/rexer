package query

import (
	"fennel/lib/ftypes"
)

type QueryRequest struct {
	QueryId      uint64           `db:"query_id"`
	Custid       ftypes.CustID    `db:"cust_id"`
	MinTimestamp ftypes.Timestamp `db:"min_timestamp"`
	MaxTimestamp ftypes.Timestamp `db:"max_timestamp"`
}

type QuerySer struct {
	QueryId   uint64           `db:"query_id"`
	Custid    ftypes.CustID    `db:"cust_id"`
	Timestamp ftypes.Timestamp `db:"timestamp"`
	QuerySer  string           `db:"query_ser"`
}
