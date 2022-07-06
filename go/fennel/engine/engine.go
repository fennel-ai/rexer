package engine

import (
	"context"
	"fennel/engine/ast"
	"fennel/engine/interpreter"
	"fennel/lib/timer"
	"fennel/lib/value"
	"fmt"
)

type QueryExecutor struct {
	bootargs map[string]interface{}
}

func NewQueryExecutor(bootargs map[string]interface{}) QueryExecutor {
	return QueryExecutor{bootargs: bootargs}
}

func (ex QueryExecutor) Exec(ctx context.Context, query ast.Ast, args value.Dict) (value.Value, error) {
	defer timer.Start("interpreter.eval").Stop()
	ip, err := interpreter.NewInterpreter(ctx, ex.bootargs, args)
	if err != nil {
		return value.Nil, fmt.Errorf("could not create interpreter: %v", err)
	}
	return query.AcceptValue(ip)
}
