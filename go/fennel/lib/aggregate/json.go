package aggregate

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/buger/jsonparser"

	"fennel/engine/ast"
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func (agg *Aggregate) UnmarshalJSON(data []byte) error {
	var fields struct {
		Name      ftypes.AggName   `json:"Name"`
		Query     string           `json:"Query"`
		Timestamp ftypes.Timestamp `json:"Timestamp"`
		Options   struct {
			AggType   string        `json:"Type"`
			Duration  uint64        `json:"Duration"`
			Window    ftypes.Window `json:"Window"`
			Limit     uint64        `json:"Limit"`
			Normalize bool          `json:"Normalize"`
		} `json:"Options"`
	}
	err := json.Unmarshal(data, &fields)
	if err != nil {
		return fmt.Errorf("error unmarshalling aggregate json: %v", err)
	}
	agg.Name = fields.Name
	agg.Timestamp = fields.Timestamp
	agg.Options.AggType = ftypes.AggType(fields.Options.AggType)
	agg.Options.Duration = fields.Options.Duration
	agg.Options.Window = fields.Options.Window
	agg.Options.Limit = fields.Options.Limit
	agg.Options.Normalize = fields.Options.Normalize
	// Extract query now
	querySer, err := base64.StdEncoding.DecodeString(fields.Query)
	if err != nil {
		return fmt.Errorf("error decoding ast from base64: %v", err)
	}
	err = ast.Unmarshal(querySer, &agg.Query)
	if err != nil {
		return fmt.Errorf("error unmarshalling ast: %v", err)
	}
	return nil
}

func (agg Aggregate) MarshalJSON() ([]byte, error) {
	querySer, err := ast.Marshal(agg.Query)
	if err != nil {
		return nil, fmt.Errorf("error marshalling ast: %v", err)
	}
	queryStr := base64.StdEncoding.EncodeToString(querySer)
	var fields struct {
		Name      ftypes.AggName   `json:"Name"`
		Query     string           `json:"Query"`
		Timestamp ftypes.Timestamp `json:"Timestamp"`
		Options   struct {
			AggType   string        `json:"Type"`
			Duration  uint64        `json:"Duration"`
			Window    ftypes.Window `json:"Window"`
			Limit     uint64        `json:"Limit"`
			Normalize bool          `json:"Normalize"`
		}
	}
	fields.Name = agg.Name
	fields.Query = queryStr
	fields.Timestamp = agg.Timestamp
	fields.Options.AggType = string(agg.Options.AggType)
	fields.Options.Duration = agg.Options.Duration
	fields.Options.Window = agg.Options.Window
	fields.Options.Limit = agg.Options.Limit
	fields.Options.Normalize = agg.Options.Normalize
	return json.Marshal(fields)
}

func (gavr *GetAggValueRequest) UnmarshalJSON(data []byte) error {
	var fields struct {
		AggName ftypes.AggName `json:"Name"`
	}
	err := json.Unmarshal(data, &fields)
	if err != nil {
		return fmt.Errorf("error unmarshalling aggvaluerequest json: %v", err)
	}
	gavr.AggName = fields.AggName
	vdata, vtype, _, err := jsonparser.Get(data, "Key")
	if err != nil {
		return fmt.Errorf("error getting key from aggvaluerequest json: %v", err)
	}
	gavr.Key, err = value.ParseJSON(vdata, vtype)
	if err != nil {
		return fmt.Errorf("error parsing key from aggvaluerequest json: %v", err)
	}
	return nil
}
