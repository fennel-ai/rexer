package action

import (
	"encoding/json"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
	"github.com/buger/jsonparser"
)

const (
	ACTIONLOG_KAFKA_TOPIC = "actionlog"
)

type Action struct {
	ActionID   ftypes.OidType    `db:"action_id"`
	ActorID    ftypes.OidType    `db:"actor_id"`
	ActorType  ftypes.OType      `db:"actor_type"`
	TargetID   ftypes.OidType    `db:"target_id"`
	TargetType ftypes.OType      `db:"target_type"`
	ActionType ftypes.ActionType `db:"action_type"`
	Timestamp  ftypes.Timestamp  `db:"timestamp"`
	RequestID  ftypes.RequestID  `db:"request_id"`
	Metadata   value.Value       `db:"metadata"`
}

type ActionSer struct {
	ActionID   ftypes.OidType    `db:"action_id"`
	ActorID    ftypes.OidType    `db:"actor_id"`
	ActorType  ftypes.OType      `db:"actor_type"`
	TargetID   ftypes.OidType    `db:"target_id"`
	TargetType ftypes.OType      `db:"target_type"`
	ActionType ftypes.ActionType `db:"action_type"`
	Timestamp  ftypes.Timestamp  `db:"timestamp"`
	RequestID  ftypes.RequestID  `db:"request_id"`
	Metadata   []byte            `db:"metadata"`
}

func (a *Action) ToActionSer() (*ActionSer, error) {
	ser := ActionSer{
		ActionID:   a.ActionID,
		ActorID:    a.ActorID,
		ActorType:  a.ActorType,
		TargetID:   a.TargetID,
		TargetType: a.TargetType,
		ActionType: a.ActionType,
		Timestamp:  a.Timestamp,
		RequestID:  a.RequestID,
	}
	valSer, err := value.Marshal(a.Metadata)
	if err != nil {
		return nil, err
	}
	ser.Metadata = valSer
	return &ser, nil
}

func (ser *ActionSer) ToAction() (*Action, error) {
	a := Action{
		ActionID:   ser.ActionID,
		ActorID:    ser.ActorID,
		ActorType:  ser.ActorType,
		TargetID:   ser.TargetID,
		TargetType: ser.TargetType,
		ActionType: ser.ActionType,
		Timestamp:  ser.Timestamp,
		RequestID:  ser.RequestID,
	}
	var val value.Value
	if err := value.Unmarshal(ser.Metadata, &val); err != nil {
		return nil, err
	}
	a.Metadata = val
	return &a, nil
}

func FromActionSerList(al_ser []ActionSer) ([]Action, error) {
	al := []Action{}
	for _, a_ser := range al_ser {
		a, err := a_ser.ToAction()
		if err != nil {
			return nil, err
		}
		al = append(al, *a)
	}
	return al, nil
}

type ActionFetchRequest struct {
	MinActionID  ftypes.OidType    `db:"min_action_id" json:"MinActionID"`
	MaxActionID  ftypes.OidType    `db:"max_action_id" json:"MaxActionID"`
	ActorID      ftypes.OidType    `db:"actor_id" json:"ActorID"`
	ActorType    ftypes.OType      `db:"actor_type" json:"ActorType"`
	TargetID     ftypes.OidType    `db:"target_id" json:"TargetID"`
	TargetType   ftypes.OType      `db:"target_type" json:"TargetType"`
	ActionType   ftypes.ActionType `db:"action_type" json:"ActionType"`
	MinTimestamp ftypes.Timestamp  `db:"min_timestamp" json:"MinTimestamp"`
	MaxTimestamp ftypes.Timestamp  `db:"max_timestamp" json:"MaxTimestamp"`
	RequestID    ftypes.RequestID  `db:"request_id" json:"RequestID"`
}

// Validate validates that all fields (except action ID) are appropriately specified
func (a *Action) Validate() error {
	if a.ActorID == 0 {
		return fmt.Errorf("actor ID can not be zero")
	}
	if len(a.ActorType) == 0 {
		return fmt.Errorf("actor type can not be empty")
	}
	if a.TargetID == 0 {
		return fmt.Errorf("target ID can not be zero")
	}
	if len(a.TargetType) == 0 {
		return fmt.Errorf("target type can not be empty")
	}
	if len(a.ActionType) == 0 {
		return fmt.Errorf("action type can not be empty")
	}
	if a.RequestID == 0 {
		return fmt.Errorf("action request ID can not be zero")
	}
	return nil
}

func (a Action) Equals(other Action, ignoreID bool) bool {
	if !ignoreID && a.ActionID != other.ActionID {
		return false
	}
	if a.ActorID != other.ActorID {
		return false
	}
	if a.ActorType != other.ActorType {
		return false
	}
	if a.TargetID != other.TargetID {
		return false
	}
	if a.TargetType != other.TargetType {
		return false
	}
	if a.ActionType != other.ActionType {
		return false
	}
	if a.Timestamp != other.Timestamp {
		return false
	}
	if a.RequestID != other.RequestID {
		return false
	}
	if a.Metadata == nil {
		if other.Metadata != nil {
			return false
		}
	} else if !a.Metadata.Equal(other.Metadata) {
		return false
	}
	return true
}

func (a Action) ToValueDict() value.Dict {
	return value.Dict{
		"action_id":   value.Int(a.ActionID),
		"actor_id":    value.Int(a.ActorID),
		"actor_type":  value.String(a.ActorType),
		"target_type": value.String(a.TargetType),
		"target_id":   value.Int(a.TargetID),
		"action_type": value.String(a.ActionType),
		"timestamp":   value.Int(a.Timestamp),
		"request_id":  value.Int(a.RequestID),
		"metadata":    a.Metadata,
	}
}

// ToTable takes a list of actions and arranges that in a value.Table if possible,
// else returns errors
func ToTable(actions []Action) (value.Table, error) {
	table := value.NewTable()
	for i := range actions {
		d := actions[i].ToValueDict()
		err := table.Append(d)
		if err != nil {
			return value.Table{}, err
		}
	}
	return table, nil
}

func (a *Action) UnmarshalJSON(data []byte) error {
	var fields struct {
		ActionID   ftypes.OidType    `json:"ActionID"`
		ActorID    ftypes.OidType    `json:"ActorID"`
		ActorType  ftypes.OType      `json:"ActorType"`
		TargetID   ftypes.OidType    `json:"TargetID"`
		TargetType ftypes.OType      `json:"TargetType"`
		ActionType ftypes.ActionType `json:"ActionType"`
		Timestamp  ftypes.Timestamp  `json:"Timestamp"`
		RequestID  ftypes.RequestID  `json:"RequestID"`
	}
	err := json.Unmarshal(data, &fields)
	if err != nil {
		return fmt.Errorf("error unmarshalling action json: %v", err)
	}
	a.ActionID = fields.ActionID
	a.ActorID = fields.ActorID
	a.ActorType = fields.ActorType
	a.TargetID = fields.TargetID
	a.TargetType = fields.TargetType
	a.ActionType = fields.ActionType
	a.Timestamp = fields.Timestamp
	a.RequestID = fields.RequestID
	vdata, vtype, _, err := jsonparser.Get(data, "Metadata")
	if err != nil {
		return fmt.Errorf("error getting metadata from action json: %v", err)
	}
	a.Metadata, err = value.ParseJSON(vdata, vtype)
	if err != nil {
		return fmt.Errorf("error parsing metadata from action json: %v", err)
	}
	return nil
}
