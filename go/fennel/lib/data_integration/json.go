package data_integration

import (
	"encoding/base64"
	"encoding/json"
	"fennel/engine/ast"
	"fmt"
)

func (conn *Connector) UnmarshalJSON(data []byte) error {
	var fields struct {
		Name        string `json:"Name"`
		SourceName  string `json:"SourceName"`
		SourceType  string `json:"SourceType"`
		Version     int    `json:"Version"`
		Destination string `json:"Destination"`
		Query       string `json:"Query"`
		StreamName  string `json:"StreamName"`
		CursorField string `json:"CursorField"`
		Active      bool   `json:"Active"`
	}
	err := json.Unmarshal(data, &fields)
	if err != nil {
		return fmt.Errorf("error unmarshalling aggregate json: %v", err)
	}
	conn.Name = fields.Name
	conn.SourceName = fields.SourceName
	conn.SourceType = fields.SourceType
	conn.Version = fields.Version
	conn.Destination = fields.Destination
	conn.Active = fields.Active
	conn.StreamName = fields.StreamName
	conn.CursorField = fields.CursorField
	// Extract query now
	querySer, err := base64.StdEncoding.DecodeString(fields.Query)
	if err != nil {
		return fmt.Errorf("error decoding ast from base64: %v", err)
	}
	err = ast.Unmarshal(querySer, &conn.Query)
	if err != nil {
		return fmt.Errorf("error unmarshalling ast: %v", err)
	}
	return nil
}
