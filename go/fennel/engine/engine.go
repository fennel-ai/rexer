package engine

import (
	"context"
	"fmt"

	"fennel/engine/ast"
	"fennel/engine/interpreter"
	"fennel/engine/interpreter/bootarg"
	"fennel/lib/timer"
	"fennel/lib/value"
)

type QueryExecutor struct {
	bootargs map[string]interface{}
}

func NewQueryExecutor(bootargs map[string]interface{}) QueryExecutor {
	return QueryExecutor{bootargs: bootargs}
}

func (ex QueryExecutor) Exec(ctx context.Context, query ast.Ast, args *value.Dict) (value.Value, error) {
	tier, err := bootarg.GetTier(ex.bootargs)
	if err != nil {
		return value.Nil, fmt.Errorf("could not get tier: %v", err)
	}
	ctx, t := timer.Start(ctx, tier.ID, "interpreter.eval")
	defer t.Stop()
	ip, err := interpreter.NewInterpreter(ctx, ex.bootargs, args)
	if err != nil {
		return value.Nil, fmt.Errorf("could not create interpreter: %v", err)
	}
	return query.AcceptValue(ip)
}
