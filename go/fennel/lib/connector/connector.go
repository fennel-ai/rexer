package connector

import (
	"fennel/engine/ast"
)

type Connector struct {
	Name        string
	SourceName  string
	SourceType  string
	Version     string
	Destination string
	Query       ast.Ast
	Config      Config
	Active      bool
}

type Config struct {
	CursorField string
}

func (Connector) Validate() error {
	return nil
}
