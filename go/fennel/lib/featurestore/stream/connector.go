package stream

import (
	"errors"
	"fmt"
)

var ErrConnNotFound = errors.New("connector not found")

type Connector struct {
	Name             string `db:"name"`
	SourceName       string `db:"source_name"`
	SourceType       string `db:"source_type"`
	StreamName       string `db:"stream_name"`
	ConnId           string `db:"conn_id"`
	Function         []byte `db:"function"`
	PopulatorSrcCode string `db:"populator_src_code"`
	CursorField      string `db:"cursor_field"`
	TableName        string `db:"table_name"`
	LastUpdated      string `db:"last_updated"`
}

func (c Connector) Validate() error {
	if len(c.Name) == 0 {
		return fmt.Errorf("connector name is required")
	}
	if len(c.SourceName) == 0 {
		return fmt.Errorf("source_name is required")
	}
	if len(c.SourceType) == 0 {
		return fmt.Errorf("source_type is required")
	}
	if len(c.StreamName) == 0 {
		return fmt.Errorf("stream_name is required")
	}
	if len(c.Function) == 0 {
		return fmt.Errorf("connector_function is required")
	}
	if len(c.TableName) == 0 {
		return fmt.Errorf("table_name is required")
	}
	if len(c.Name) > 255 {
		return fmt.Errorf("connector name cannot be longer than 255 characters")
	}
	if len(c.SourceName) > 255 {
		return fmt.Errorf("source_name cannot be longer than 255 characters")
	}
	if len(c.SourceType) > 255 {
		return fmt.Errorf("source_type cannot be longer than 255 characters")
	}
	if len(c.StreamName) > 255 {
		return fmt.Errorf("stream_name cannot be longer than 255 characters")
	}
	if len(c.CursorField) > 255 {
		return fmt.Errorf("cursor_field cannot be longer than 255 characters")
	}
	if len(c.TableName) > 255 {
		return fmt.Errorf("table_name cannot be longer than 255 characters")
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
	if c.StreamName != other.StreamName {
		return fmt.Errorf("stream_name mismatch")
	}
	for i := range c.Function {
		if c.Function[i] != other.Function[i] {
			return fmt.Errorf("connector_function mismatch")
		}
	}
	if c.CursorField != other.CursorField {
		return fmt.Errorf("cursor_field mismatch")
	}
	if c.TableName != other.TableName {
		return fmt.Errorf("table_name mismatch")
	}
	return nil
}
