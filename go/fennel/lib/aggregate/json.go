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
		Id        ftypes.AggId     `json:"Id"`
		Active    bool             `json:"Active"`
		Query     string           `json:"Query"`
		Timestamp ftypes.Timestamp `json:"Timestamp"`
		Source    ftypes.Source    `json:"Source"`
		Mode      string           `json:"Mode"`
		Options   struct {
			AggType         string        `json:"Type"`
			Durations       []uint32      `json:"Durations"`
			Window          ftypes.Window `json:"Window"`
			Limit           uint32        `json:"Limit"`
			Normalize       bool          `json:"Normalize"`
			CronSchedule    string        `json:"CronSchedule"`
			Dim             uint32        `json:"Dim"`
			HyperParameters string        `json:"HyperParameters"`
		} `json:"Options"`
	}
	err := json.Unmarshal(data, &fields)
	if err != nil {
		return fmt.Errorf("error unmarshalling aggregate json: %v", err)
	}
	agg.Name = fields.Name
	agg.Id = fields.Id
	agg.Active = fields.Active
	agg.Timestamp = fields.Timestamp
	agg.Source = fields.Source
	agg.Options.AggType = ftypes.AggType(fields.Options.AggType)
	agg.Options.Durations = fields.Options.Durations
	agg.Options.Window = fields.Options.Window
	agg.Options.Limit = fields.Options.Limit
	agg.Options.Normalize = fields.Options.Normalize
	agg.Options.CronSchedule = fields.Options.CronSchedule
	agg.Options.Dim = fields.Options.Dim
	agg.Options.HyperParameters = fields.Options.HyperParameters
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
		Id        ftypes.AggId     `json:"Id"`
		Active    bool             `json:"Active"`
		Query     string           `json:"Query"`
		Mode      string           `json:"Mode"`
		Timestamp ftypes.Timestamp `json:"Timestamp"`
		Source    ftypes.Source    `json:"Source"`
		Options   struct {
			AggType         string        `json:"Type"`
			Durations       []uint32      `json:"Durations"`
			Window          ftypes.Window `json:"Window"`
			Limit           uint32        `json:"Limit"`
			Normalize       bool          `json:"Normalize"`
			CronSchedule    string        `json:"CronSchedule"`
			Dim             uint32        `json:"Dim"`
			HyperParameters string        `json:"HyperParameters"`
		}
	}
	fields.Name = agg.Name
	fields.Id = agg.Id
	fields.Active = agg.Active
	fields.Query = queryStr
	fields.Timestamp = agg.Timestamp
	fields.Source = agg.Source
	fields.Options.AggType = string(agg.Options.AggType)
	fields.Options.Durations = agg.Options.Durations
	fields.Options.Window = agg.Options.Window
	fields.Options.Limit = agg.Options.Limit
	fields.Options.Normalize = agg.Options.Normalize
	fields.Options.CronSchedule = agg.Options.CronSchedule
	fields.Options.Dim = agg.Options.Dim
	fields.Options.HyperParameters = agg.Options.HyperParameters
	return json.Marshal(fields)
}

func (gavr GetAggValueRequest) MarshalJSON() ([]byte, error) {
	type GAVR_ GetAggValueRequest
	gavr_ := GAVR_(gavr)
	gavr_.Key = value.Clean(gavr.Key)
	gavr_.Kwargs = value.Clean(gavr.Kwargs).(value.Dict)
	return json.Marshal(gavr_)
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
	vdata, vtype, _, err = jsonparser.Get(data, "Kwargs")
	if err != nil {
		return fmt.Errorf("error getting kwargs from aggvaluerequest json: %v", err)
	}
	kwargs, err := value.ParseJSON(vdata, vtype)
	if err != nil {
		return fmt.Errorf("error parsing kwargs from aggvaluerequest json: %v", err)
	}
	var ok bool
	if gavr.Kwargs, ok = kwargs.(value.Dict); !ok {
		return fmt.Errorf("kwargs in aggvaluerequest json should be dict but found: '%v'", kwargs)
	}
	return nil
}
