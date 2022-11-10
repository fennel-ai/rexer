package query

import (
	"context"
	libquery "fennel/lib/query"
	"fmt"
	"go.uber.org/zap"
	"time"

	"fennel/engine/ast"
	"fennel/lib/ftypes"
	"fennel/model/query"
	"fennel/tier"
)

const cacheValueDuration = 2 * time.Minute

func Insert(ctx context.Context, tier tier.Tier, name string, tree ast.Ast, description string) (uint64, error) {
	ret, err := query.Retrieve(ctx, tier, name)
	if err == nil {
		var tree2 ast.Ast
		err = ast.Unmarshal(ret.QuerySer, &tree2)
		if ret.Description == description && err == nil && tree2.Equals(tree) {
			return ret.QueryId, nil
		}
		return 0, fmt.Errorf("query with name '%s' already exists with a different config", name)
	}
	if err != query.ErrNotFound {
		return 0, fmt.Errorf("failed to get query: %w", err)
	}
	ts := ftypes.Timestamp(tier.Clock.Now().Unix())
	treeSer, err := ast.Marshal(tree)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal ast: %w", err)
	}
	return query.Insert(tier, name, ts, treeSer, description)
}

func Get(ctx context.Context, tier tier.Tier, name string) (ast.Ast, error) {
	// if found in cache, return directly
	if v, ok := tier.PCache.Get(name, "QueryStore"); ok {
		if tree, ok := fromCacheValue(tier, v); ok {
			return tree, nil
		}
	}
	// otherwise, store in cache and return
	ret, err := query.Retrieve(ctx, tier, name)
	if err == query.ErrNotFound {
		return nil, fmt.Errorf("query with name '%s' not found", name)
	} else if err != nil {
		return nil, fmt.Errorf("failed to get query: %w", err)
	}

	var tree ast.Ast
	err = ast.Unmarshal(ret.QuerySer, &tree)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshall ast: %w", err)
	}
	if !tier.PCache.SetWithTTL(name, tree, 0, cacheValueDuration, "QueryStore") {
		tier.Logger.Debug(fmt.Sprintf("failed to set query in cache: key: '%s' value: '%v'", name, tree))
	}
	return tree, nil
}

func List(ctx context.Context, tier tier.Tier) ([]libquery.QuerySer, error) {
	return query.RetrieveAll(ctx, tier)
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
