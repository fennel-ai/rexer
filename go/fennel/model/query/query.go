package query

import (
	"context"
	"database/sql"
	"errors"
	"fennel/lib/ftypes"
	"fennel/lib/query"
	"fennel/tier"
)

var ErrNotFound = errors.New("Query not found")

func Insert(tier tier.Tier, name string, timestamp ftypes.Timestamp, querySer []byte, description string) (uint64, error) {
	sql := "INSERT INTO query_ast (name, timestamp, query_ser, description) VALUES (?, ?, ?, ?);"
	res, err := tier.DB.Exec(sql, name, timestamp, querySer, description)
	if err != nil {
		return 0, err
	}
	queryID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint64(queryID), nil
}

func Retrieve(ctx context.Context, tier tier.Tier, name string) (query.QuerySer, error) {
	var query query.QuerySer
	err := tier.DB.GetContext(ctx, &query, "SELECT * FROM query_ast WHERE name = ? limit 1", name)
	if err != nil && err == sql.ErrNoRows {
		return query, ErrNotFound
	} else if err != nil {
		return query, err
	}
	return query, nil
}

func RetrieveAll(ctx context.Context, tier tier.Tier) ([]query.QuerySer, error) {
	var queries []query.QuerySer
	err := tier.DB.SelectContext(ctx, &queries, "SELECT * FROM query_ast")
	if err != nil {
		return nil, err
	}
	return queries, nil
}
