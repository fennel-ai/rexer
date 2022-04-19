package action

import (
	"encoding/json"
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/buger/jsonparser"
)

const (
	ACTIONLOG_KAFKA_TOPIC = "actionlog"
	// NOTE: `actionlog_json` is used to log actions as "labels" of the training dataset;
	ACTIONLOG_JSON_KAFKA_TOPIC = "actionlog_json"
)

type Action struct {
	ActionID   ftypes.IDType     `db:"action_id"`
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
	ActionID   ftypes.IDType     `db:"action_id"`
	ActorID    ftypes.OidType    `db:"actor_id"`
	ActorType  ftypes.OType      `db:"actor_type"`
	TargetID   ftypes.OidType    `db:"target_id"`
	TargetType ftypes.OType      `db:"target_type"`
	ActionType ftypes.ActionType `db:"action_type"`
	Timestamp  ftypes.Timestamp  `db:"timestamp"`
	RequestID  ftypes.RequestID  `db:"request_id"`
	Metadata   []byte            `db:"metadata"`
}

func (a *Action) ToActionSer() *ActionSer {
	return &ActionSer{
		ActionID:   a.ActionID,
		ActorID:    a.ActorID,
		ActorType:  a.ActorType,
		TargetID:   a.TargetID,
		TargetType: a.TargetType,
		ActionType: a.ActionType,
		Timestamp:  a.Timestamp,
		RequestID:  a.RequestID,
		Metadata:   value.ToJSON(a.Metadata),
	}
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
	val, err := value.FromJSON(ser.Metadata)
	if err != nil {
		return nil, err
	}
	a.Metadata = val
	return &a, nil
}

func FromActionSerList(alSer []ActionSer) ([]Action, error) {
	al := make([]Action, len(alSer))
	for i, aSer := range alSer {
		a, err := aSer.ToAction()
		if err != nil {
			return nil, err
		}
		al[i] = *a
	}
	return al, nil
}

type ActionFetchRequest struct {
	MinActionID  ftypes.IDType     `db:"min_action_id" json:"MinActionID"`
	MaxActionID  ftypes.IDType     `db:"max_action_id" json:"MaxActionID"`
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
	if len(a.ActorID) == 0 {
		return fmt.Errorf("actor ID can not be empty")
	}
	if len(a.ActorType) == 0 {
		return fmt.Errorf("actor type can not be empty")
	}
	if len(a.TargetID) == 0 {
		return fmt.Errorf("target ID can not be empty")
	}
	if len(a.TargetType) == 0 {
		return fmt.Errorf("target type can not be empty")
	}
	if len(a.ActionType) == 0 {
		return fmt.Errorf("action type can not be empty")
	}
	if len(a.RequestID) == 0 {
		return fmt.Errorf("action request ID can not be empty")
	}
	if len(a.ActionType) > 255 {
		return fmt.Errorf("action type too long: action types cannot be longer than 255 chars")
	}
	if len(a.ActorType) > 255 {
		return fmt.Errorf("actor type too long: actor types cannot be longer than 255 chars")
	}
	if len(a.TargetType) > 255 {
		return fmt.Errorf("target type too long: target types cannot be longer than 255 chars")
	}
	// if a.Timestamp > 0 && int64(a.Timestamp)-time.Now().Unix() > 60 {
	// 	return fmt.Errorf("action timestamp in the future, ensure that timestamp is in seconds %d", a.Timestamp)
	// }
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
	return value.NewDict(map[string]value.Value{
		"action_id":   value.Int(a.ActionID),
		"actor_id":    value.String(a.ActorID),
		"actor_type":  value.String(a.ActorType),
		"target_type": value.String(a.TargetType),
		"target_id":   value.String(a.TargetID),
		"action_type": value.String(a.ActionType),
		"timestamp":   value.Int(a.Timestamp),
		"request_id":  value.String(a.RequestID),
		"metadata":    a.Metadata,
	})
}

// ToList takes a list of actions and arranges that in a value.List
// else returns errors
func ToList(actions []Action) (value.List, error) {
	table := value.List{}
	for i := range actions {
		d := actions[i].ToValueDict()
		table.Append(d)
	}
	return table, nil
}

func (a Action) MarshalJSON() ([]byte, error) {
	type Action_ Action
	a_ := Action_(a)
	a_.Metadata = value.Clean(a.Metadata)
	return json.Marshal(a_)
}

func (a *Action) UnmarshalJSON(data []byte) error {
	var fields struct {
		ActionID   ftypes.IDType     `json:"ActionID"`
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
