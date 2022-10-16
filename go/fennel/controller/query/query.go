package query

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"fennel/engine/ast"
	"fennel/lib/ftypes"
	libquery "fennel/lib/query"
	"fennel/model/query"
	"fennel/tier"
)

const cacheValueDuration = 2 * time.Minute

func Insert(tier tier.Tier, name string, tree ast.Ast) (uint64, error) {
	ts := ftypes.Timestamp(tier.Clock.Now().Unix())
	treeSer, err := ast.Marshal(tree)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal ast: %w", err)
	}
	return query.Insert(tier, name, ts, treeSer)
}

func Get(tier tier.Tier, name string) (ast.Ast, error) {
	// if found in cache, return directly
	if v, ok := tier.PCache.Get(name, "QueryStore"); ok {
		if tree, ok := fromCacheValue(tier, v); ok {
			return tree, nil
		}
	}
	// otherwise, store in cache and return
	ret, err := query.Get(tier, libquery.QueryRequest{Name: name})
	if err != nil {
		return nil, fmt.Errorf("failed to get query: %w", err)
	}
	if len(ret) == 0 {
		return nil, fmt.Errorf("query with name '%s' not found", name)
	}
	var tree ast.Ast
	err = ast.Unmarshal(ret[0].QuerySer, &tree)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshall ast: %w", err)
	}
	if !tier.PCache.SetWithTTL(name, tree, 0, cacheValueDuration, "QueryStore") {
		tier.Logger.Debug(fmt.Sprintf("failed to set query in cache: key: '%s' value: '%v'", name, tree))
	}
	return tree, nil
}

func fromCacheValue(tier tier.Tier, v interface{}) (ast.Ast, bool) {
	switch v := v.(type) {
	case ast.Ast:
		return v, true
	default:
		// log unexpected error
		err := fmt.Errorf("value not of type stringe: %v", v)
		tier.Logger.Error("query cache error: ", zap.Error(err))
		return nil, false
	}
}
