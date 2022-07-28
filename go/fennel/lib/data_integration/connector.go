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
	Version     int
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
	if c.Destination != "action" && c.Destination != "profile" {
		return fmt.Errorf("invalid destination: %s", c.Destination)
	}
	if c.Query == nil {
		return fmt.Errorf("query is required")
	}
	return nil
}

func (c Connector) Equals(other Connector) error {
	if c.Name != other.Name {
		return fmt.Errorf("name mismatch")
	}
	if c.SourceName != other.SourceName {
		return fmt.Errorf("source_name mismatch")
	}
	if c.SourceType != other.SourceType {
		return fmt.Errorf("source_type mismatch")
	}
	if c.Version != other.Version {
		return fmt.Errorf("version mismatch")
	}
	if c.Destination != other.Destination {
		return fmt.Errorf("destination mismatch")
	}
	if c.CursorField != other.CursorField {
		return fmt.Errorf("cursor_field mismatch")
	}
	if !c.Query.Equals(other.Query) {
		return fmt.Errorf("query mismatch")
	}
	return nil
}
