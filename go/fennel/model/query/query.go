package query

import (
	"fmt"
	"strings"

	"fennel/lib/ftypes"
	"fennel/lib/query"
	"fennel/tier"
)

func Insert(tier tier.Tier, name string, timestamp ftypes.Timestamp, querySer []byte) (uint64, error) {
	sql := "INSERT INTO query_ast (name, timestamp, query_ser) VALUES (?, ?, ?);"
	res, err := tier.DB.Exec(sql, name, timestamp, querySer)
	if err != nil {
		return 0, err
	}
	queryID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint64(queryID), nil
}

func Get(tier tier.Tier, request query.QueryRequest) ([]query.QuerySer, error) {
	sql := "SELECT * FROM query_ast"
	clauses := make([]string, 0)
	if request.QueryId > 0 {
		clauses = append(clauses, "query_id = :query_id")
	}
	if request.Name != "" {
		clauses = append(clauses, "name = :name")
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
	statement, err := tier.DB.PrepareNamed(sql)
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
