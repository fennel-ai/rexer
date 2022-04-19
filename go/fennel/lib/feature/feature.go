package feature

import (
	"encoding/json"
	"fmt"
	"strings"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

const (
	KAFKA_TOPIC_NAME = "featurelog"
)

type Row struct {
	ContextOType    ftypes.OType        `json:"context_otype"`
	ContextOid      ftypes.IDType       `json:"context_oid"`
	CandidateOType  ftypes.OType        `json:"candidate_otype"`
	CandidateOid    ftypes.IDType       `json:"candidate_oid"`
	Features        value.Dict          `json:"data"`
	Workflow        string              `json:"workflow"`
	RequestID       ftypes.RequestID    `json:"request_id"`
	Timestamp       ftypes.Timestamp    `json:"timestamp"`
	ModelName       ftypes.ModelName    `json:"model_name"`
	ModelVersion    ftypes.ModelVersion `json:"model_version"`
	ModelPrediction float64             `json:"model_prediction"`
}

func (r *Row) UnmarshalJSON(bytes []byte) error {
	d, err := value.FromJSON(bytes)
	if err != nil {
		return err
	}
	asdict, ok := d.(value.Dict)
	if !ok {
		return fmt.Errorf("can not unmarshal feature row - expected dict but found: %v", d)
	}
	r.Features = value.NewDict(make(map[string]value.Value))
	for k, v := range asdict.Iter() {
		if len(k) == 0 {
			return fmt.Errorf("can not unmarshal feature row, json contains empty key")
		}
		switch k {
		case "workflow":
			s, ok := v.(value.String)
			if !ok {
				return fmt.Errorf("can not unmarshal feature row, expected string for workflow but found: %v", v)
			}
			r.Workflow = string(s)
		case "model_name":
			s, ok := v.(value.String)
			if !ok {
				return fmt.Errorf("can not unmarshal feature row, expected string for model_name but found: %v", v)
			}
			r.ModelName = ftypes.ModelName(s)
		case "model_version":
			s, ok := v.(value.String)
			if !ok {
				return fmt.Errorf("can not unmarshal feature row, expected string for model_version but found: %v", v)
			}
			r.ModelVersion = ftypes.ModelVersion(s)
		case "request_id":
			n, ok := v.(value.Int)
			if !ok {
				return fmt.Errorf("can not unmarshal feature row, expected integer for request_id but found: %v", v)
			}
			r.RequestID = ftypes.RequestID(n)
		case "context_otype":
			s, ok := v.(value.String)
			if !ok {
				return fmt.Errorf("can not unmarshal feature row, expected string for context_otype but found: %v", v)
			}
			r.ContextOType = ftypes.OType(s)
		case "context_oid":
			n, ok := v.(value.Int)
			if !ok {
				return fmt.Errorf("can not unmarshal feature row, expected integer for context_oid but found: %v", v)
			}
			r.ContextOid = ftypes.IDType(n)
		case "candidate_otype":
			s, ok := v.(value.String)
			if !ok {
				return fmt.Errorf("can not unmarshal feature row, expected string for context_otype but found: %v", v)
			}
			r.CandidateOType = ftypes.OType(s)
		case "candidate_oid":
			n, ok := v.(value.Int)
			if !ok {
				return fmt.Errorf("can not unmarshal feature row, expected integer for candidate_id but found: %v", v)
			}
			r.CandidateOid = ftypes.IDType(n)
		case "timestamp":
			n, ok := v.(value.Int)
			if !ok {
				return fmt.Errorf("can not unmarshal feature row, expected integer for timestamp but found: %v", v)
			}
			r.Timestamp = ftypes.Timestamp(n)
		case "model_prediction":
			switch p := v.(type) {
			case value.Double:
				r.ModelPrediction = float64(p)
			case value.Int:
				r.ModelPrediction = float64(p)
			default:
				return fmt.Errorf("can not unmarshal feature row, expected float for model_prediction but found: %v", v)
			}
		default:
			pieces := strings.SplitN(k, "__", 2)
			if len(pieces) != 2 || pieces[0] != "feature" {
				return fmt.Errorf("can not unmarshal feature row, invalid field: %v", k)
			}
			r.Features.Set(pieces[1], v)
		}
	}
	return nil
}

func (r Row) GetValue() value.Value {
	d := value.NewDict(map[string]value.Value{})
	for k, v := range r.Features.Iter() {
		pk := prefixed("feature", k)
		d.Set(pk, v)
	}
	d.Set("context_otype", value.String(r.ContextOType))
	d.Set("context_oid", value.Int(r.ContextOid))
	d.Set("candidate_otype", value.String(r.CandidateOType))
	d.Set("candidate_oid", value.Int(r.CandidateOid))
	d.Set("timestamp", value.Int(r.Timestamp))
	d.Set("workflow", value.String(r.Workflow))
	d.Set("request_id", value.Int(r.RequestID))
	d.Set("model_name", value.String(r.ModelName))
	d.Set("model_version", value.String(r.ModelVersion))
	d.Set("model_prediction", value.Double(r.ModelPrediction))
	return d
}

func (r Row) MarshalJSON() ([]byte, error) {
	d := r.GetValue()
	return value.ToJSON(d), nil
}

func prefixed(prefix, k string) string {
	return fmt.Sprintf("%s__%s", prefix, k)
}

var _ json.Marshaler = Row{}
var _ json.Unmarshaler = &Row{}

func FromProtoRow(pr *ProtoRow) (*Row, error) {
	pdata, err := value.FromProtoValue(pr.Features)
	if err != nil {
		return nil, err
	}
	asdict, ok := pdata.(value.Dict)
	if !ok {
		return nil, fmt.Errorf("invalid value, expected dictionary but found: %v", pdata)
	}
	return &Row{
		ContextOType:    ftypes.OType(pr.ContextOType),
		ContextOid:      ftypes.IDType(pr.ContextOid),
		CandidateOType:  ftypes.OType(pr.CandidateOType),
		CandidateOid:    ftypes.IDType(pr.CandidateOid),
		Features:        asdict,
		Workflow:        pr.Workflow,
		RequestID:       ftypes.RequestID(pr.RequestID),
		Timestamp:       ftypes.Timestamp(pr.Timestamp),
		ModelName:       ftypes.ModelName(pr.ModelName),
		ModelVersion:    ftypes.ModelVersion(pr.ModelVersion),
		ModelPrediction: pr.ModelPrediction,
	}, nil
}

func ToProto(r Row) (*ProtoRow, error) {
	pv, err := value.ToProtoValue(r.Features)
	if err != nil {
		return nil, err
	}
	return &ProtoRow{
		ContextOType:    string(r.ContextOType),
		ContextOid:      uint64(r.ContextOid),
		CandidateOType:  string(r.CandidateOType),
		CandidateOid:    uint64(r.CandidateOid),
		Features:        &pv,
		Workflow:        r.Workflow,
		RequestID:       uint64(r.RequestID),
		Timestamp:       uint64(r.Timestamp),
		ModelName:       string(r.ModelName),
		ModelVersion:    string(r.ModelVersion),
		ModelPrediction: r.ModelPrediction,
	}, nil
}
