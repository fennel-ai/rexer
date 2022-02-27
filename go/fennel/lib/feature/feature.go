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
	ContextOType    ftypes.OType     `json:"context_otype"`
	ContextOid      ftypes.OidType   `json:"context_oid"`
	CandidateOType  ftypes.OType     `json:"candidate_otype"`
	CandidateOid    ftypes.OidType   `json:"candidate_oid"`
	Features        value.Dict       `json:"data"`
	Workflow        string           `json:"workflow"`
	RequestID       ftypes.RequestID `json:"request_id"`
	Timestamp       ftypes.Timestamp `json:"timestamp"`
	ModelID         ftypes.ModelID   `json:"model_id"`
	ModelPrediction float64          `json:"model_prediction"`
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
	r.Features = make(map[string]value.Value)
	for k, v := range asdict {
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
		case "model_id":
			s, ok := v.(value.String)
			if !ok {
				return fmt.Errorf("can not unmarshal feature row, expected string for model_id but found: %v", v)
			}
			r.ModelID = ftypes.ModelID(s)
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
			r.ContextOid = ftypes.OidType(n)
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
			r.CandidateOid = ftypes.OidType(n)
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
			r.Features[pieces[1]] = v
		}
	}
	return nil
}

func (r Row) MarshalJSON() ([]byte, error) {
	d := value.Dict{}
	for k, v := range r.Features {
		pk := prefixed("feature", k)
		d[pk] = v
	}
	d["context_otype"] = value.String(r.ContextOType)
	d["context_oid"] = value.Int(r.ContextOid)
	d["candidate_otype"] = value.String(r.CandidateOType)
	d["candidate_oid"] = value.Int(r.CandidateOid)
	d["timestamp"] = value.Int(r.Timestamp)
	d["workflow"] = value.String(r.Workflow)
	d["model_id"] = value.String(r.ModelID)
	d["request_id"] = value.Int(r.RequestID)
	d["model_prediction"] = value.Double(r.ModelPrediction)
	return value.ToJSON(d)
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
		ContextOid:      ftypes.OidType(pr.ContextOid),
		CandidateOType:  ftypes.OType(pr.CandidateOType),
		CandidateOid:    ftypes.OidType(pr.CandidateOid),
		Features:        asdict,
		Workflow:        pr.Workflow,
		RequestID:       ftypes.RequestID(pr.RequestID),
		Timestamp:       ftypes.Timestamp(pr.Timestamp),
		ModelID:         ftypes.ModelID(pr.ModelID),
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
		ModelID:         string(r.ModelID),
		ModelPrediction: r.ModelPrediction,
	}, nil
}
