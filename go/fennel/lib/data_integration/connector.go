package data_integration

import (
	"fennel/engine/ast"
	"fmt"
)

type Connector struct {
	Name        string
	SourceName  string
	SourceType  string
	Version     string
	Destination string
	Query       ast.Ast
	Active      bool
}

func (c Connector) Validate() error {
	if c.SourceName == "" {
		return fmt.Errorf("source_name is required")
	}
	return nil
}

func (c Connector) Equals(other Connector) bool {
	return c.Name == other.Name &&
		c.SourceName == other.SourceName &&
		c.SourceType == other.SourceType &&
		c.Version == other.Version &&
		c.Destination == other.Destination &&
		c.Active == other.Active &&
		c.Query.Equals(other.Query)
}
