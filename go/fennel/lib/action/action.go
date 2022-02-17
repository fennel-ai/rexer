package action

import (
	"encoding/json"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
	"github.com/buger/jsonparser"
	"google.golang.org/protobuf/proto"
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

func FromProtoAction(pa *ProtoAction) (Action, error) {
	v, err := value.FromProtoValue(pa.GetMetadata())
	if err != nil {
		return Action{}, err
	}
	return Action{
		ActionID:   ftypes.OidType(pa.GetActionID()),
		ActorID:    ftypes.OidType(pa.GetActorID()),
		ActorType:  ftypes.OType(pa.GetActorType()),
		TargetID:   ftypes.OidType(pa.GetTargetID()),
		TargetType: ftypes.OType(pa.GetTargetType()),
		ActionType: ftypes.ActionType(pa.GetActionType()),
		Timestamp:  ftypes.Timestamp(pa.GetTimestamp()),
		RequestID:  ftypes.RequestID(pa.RequestID),
		Metadata:   v,
	}, nil
}

func ToProtoAction(a Action) (ProtoAction, error) {
	pv, err := value.ToProtoValue(a.Metadata)
	if err != nil {
		return ProtoAction{}, err
	}
	return ProtoAction{
		ActionID:   uint64(a.ActionID),
		ActorID:    uint64(a.ActorID),
		ActorType:  string(a.ActorType),
		TargetID:   uint64(a.TargetID),
		TargetType: string(a.TargetType),
		ActionType: string(a.ActionType),
		Timestamp:  uint64(a.Timestamp),
		RequestID:  uint64(a.RequestID),
		Metadata:   &pv,
	}, nil
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

	pval, err := value.ToProtoValue(a.Metadata)
	if err != nil {
		return nil, err
	}
	val_ser, err := proto.Marshal(&pval)
	if err != nil {
		return nil, err
	}

	ser.Metadata = val_ser
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

	var pval value.PValue
	if err := proto.Unmarshal(ser.Metadata, &pval); err != nil {
		return nil, err
	}

	val, err := value.FromProtoValue(&pval)
	if err != nil {
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

func FromProtoActionFetchRequest(pa *ProtoActionFetchRequest) ActionFetchRequest {
	return ActionFetchRequest{

		MinActionID:  ftypes.OidType(pa.GetMinActionID()),
		MaxActionID:  ftypes.OidType(pa.GetMaxActionID()),
		ActorID:      ftypes.OidType(pa.GetActorID()),
		ActorType:    ftypes.OType(pa.GetActorType()),
		TargetID:     ftypes.OidType(pa.GetTargetID()),
		TargetType:   ftypes.OType(pa.GetTargetType()),
		ActionType:   ftypes.ActionType(pa.GetActionType()),
		MinTimestamp: ftypes.Timestamp(pa.GetMinTimestamp()),
		MaxTimestamp: ftypes.Timestamp(pa.GetMaxTimestamp()),
		RequestID:    ftypes.RequestID(pa.GetRequestID()),
	}
}

func ToProtoActionFetchRequest(a ActionFetchRequest) ProtoActionFetchRequest {
	return ProtoActionFetchRequest{

		MinActionID:  uint64(a.MinActionID),
		MaxActionID:  uint64(a.MaxActionID),
		ActorID:      uint64(a.ActorID),
		ActorType:    string(a.ActorType),
		TargetID:     uint64(a.TargetID),
		TargetType:   string(a.TargetType),
		ActionType:   string(a.ActionType),
		MinTimestamp: uint64(a.MinTimestamp),
		MaxTimestamp: uint64(a.MaxTimestamp),
		RequestID:    uint64(a.RequestID),
	}
}

func FromProtoActionList(actionList *ProtoActionList) ([]Action, error) {
	actions := make([]Action, len(actionList.Actions))
	var err error
	for i, pa := range actionList.Actions {
		actions[i], err = FromProtoAction(pa)
		if err != nil {
			return nil, err
		}
	}
	return actions, nil
}

func ToProtoActionList(actions []Action) (*ProtoActionList, error) {
	ret := &ProtoActionList{}
	ret.Actions = make([]*ProtoAction, len(actions))
	for i, action := range actions {
		pa, err := ToProtoAction(action)
		if err != nil {
			return nil, err
		}
		ret.Actions[i] = &pa
	}
	return ret, nil
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

func (a Action) Equals(other Action, ignoreID bool) (bool, error) {
	if a.Metadata == nil || other.Metadata == nil {
		return false, fmt.Errorf("metadata of action should be value.Nil not nil pointer")
	}
	if !ignoreID && a.ActionID != other.ActionID {
		return false, nil
	}
	if a.ActorID != other.ActorID {
		return false, nil
	}
	if a.ActorType != other.ActorType {
		return false, nil
	}
	if a.TargetID != other.TargetID {
		return false, nil
	}
	if a.TargetType != other.TargetType {
		return false, nil
	}
	if a.ActionType != other.ActionType {
		return false, nil
	}
	if a.Timestamp != other.Timestamp {
		return false, nil
	}
	if a.RequestID != other.RequestID {
		return false, nil
	}
	if !a.Metadata.Equal(other.Metadata) {
		return false, nil
	}
	return true, nil
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
