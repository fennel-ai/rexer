package query

import (
	"fennel/lib/ftypes"
	"fennel/lib/query"
	"fennel/tier"
	"fmt"
	"strings"
)

func Insert(instance tier.Tier, custid ftypes.CustID, timestamp ftypes.Timestamp, querySer string) (uint64, error) {
	sql := "INSERT INTO query_ast VALUES (?, ?, ?, ?);"
	res, err := instance.DB.Exec(sql, 0, custid, timestamp, querySer)
	if err != nil {
		return 0, err
	}
	queryID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint64(queryID), nil
}

func Get(instance tier.Tier, request query.QueryRequest) ([]query.QuerySer, error) {
	sql := "SELECT * FROM query_ast"
	clauses := make([]string, 0)
	if request.QueryId > 0 {
		clauses = append(clauses, "query_id = :query_id")
	}
	if request.Custid > 0 {
		clauses = append(clauses, "cust_id = :cust_id")
	}
	if request.MinTimestamp > 0 {
		clauses = append(clauses, "timestamp >= :min_timestamp")
	}
	if request.MaxTimestamp > 0 {
		clauses = append(clauses, "timestamp < :max_timestamp")
	}
	if len(clauses) > 0 {
		sql = fmt.Sprintf("%s WHERE %s", sql, strings.Join(clauses, " AND "))
	}
	queries := make([]query.QuerySer, 0)
	statement, err := instance.DB.PrepareNamed(sql)
	if err != nil {
		return nil, err
	}
	err = statement.Select(&queries, request)
	if err != nil {
		return nil, err
	} else {
		return queries, nil
	}
}
