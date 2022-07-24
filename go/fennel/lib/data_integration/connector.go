package data_integration

import (
	"errors"
	"fennel/engine/ast"
	"fmt"
)

var ErrConnNotFound = errors.New("connector not found")

type Connector struct {
	Name        string
	SourceName  string
	SourceType  string
	StreamName  string
	Version     string
	Destination string
	ConnId      string
	CursorField string
	Query       ast.Ast
	Active      bool
}

func (c Connector) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("connector name is required")
	}
	if c.SourceName == "" {
		return fmt.Errorf("source_name is required")
	}
	if c.SourceType == "" {
		return fmt.Errorf("source_type is required")
	}
	if c.Version == "" {
		return fmt.Errorf("version is required")
	}
	if c.Destination != "actions" && c.Destination != "profiles" {
		return fmt.Errorf("invalid destination: %s", c.Destination)
	}
	if c.Query == nil {
		return fmt.Errorf("query is required")
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
