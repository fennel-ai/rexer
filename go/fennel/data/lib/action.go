package lib

import "fmt"

// TODO: add freeform text field for metadata?
type Action struct {
	ActionID    OidType    `db:"action_id"`
	ActorID     OidType    `db:"actor_id"`
	ActorType   OType      `db:"actor_type"`
	TargetID    OidType    `db:"target_id"`
	TargetType  OType      `db:"target_type"`
	ActionType  ActionType `db:"action_type"`
	ActionValue int32      `db:"action_value"`
	Timestamp   Timestamp  `db:"timestamp"`
	RequestID   RequestID  `db:"request_id"`
}

func FromProtoAction(pa *ProtoAction) Action {
	return Action{
		ActionID:    OidType(pa.GetActionID()),
		ActorID:     OidType(pa.GetActorID()),
		ActorType:   OType(pa.GetActorType()),
		TargetID:    OidType(pa.GetTargetID()),
		TargetType:  OType(pa.GetTargetType()),
		ActionType:  ActionType(pa.GetActionType()),
		ActionValue: pa.GetActionValue(),
		Timestamp:   Timestamp(pa.GetTimestamp()),
		RequestID:   RequestID(pa.RequestID),
	}
}

func ToProtoAction(a Action) ProtoAction {
	return ProtoAction{
		ActionID:    uint64(a.ActionID),
		ActorID:     uint64(a.ActorID),
		ActorType:   uint32(a.ActorType),
		TargetID:    uint64(a.TargetID),
		TargetType:  uint32(a.TargetType),
		ActionType:  uint32(a.ActionType),
		ActionValue: a.ActionValue,
		Timestamp:   uint64(a.Timestamp),
		RequestID:   uint64(a.RequestID),
	}
}

type ActionFetchRequest struct {
	MinActionID    OidType    `db:"min_action_id"`
	MaxActionID    OidType    `db:"max_action_id"`
	ActorID        OidType    `db:"actor_id"`
	ActorType      OType      `db:"actor_type"`
	TargetID       OidType    `db:"target_id"`
	TargetType     OType      `db:"target_type"`
	ActionType     ActionType `db:"action_type"`
	MinActionValue int32      `db:"min_action_value"`
	MaxActionValue int32      `db:"max_action_value"`
	MinTimestamp   Timestamp  `db:"min_timestamp"`
	MaxTimestamp   Timestamp  `db:"max_timestamp"`
	RequestID      RequestID  `db:"request_id"`
}

func FromProtoActionFetchRequest(pa *ProtoActionFetchRequest) ActionFetchRequest {
	return ActionFetchRequest{

		MinActionID:    OidType(pa.GetMinActionID()),
		MaxActionID:    OidType(pa.GetMaxActionID()),
		ActorID:        OidType(pa.GetActorID()),
		ActorType:      OType(pa.GetActorType()),
		TargetID:       OidType(pa.GetTargetID()),
		TargetType:     OType(pa.GetTargetType()),
		ActionType:     ActionType(pa.GetActionType()),
		MinActionValue: pa.GetMinActionValue(),
		MaxActionValue: pa.GetMaxActionValue(),
		MinTimestamp:   Timestamp(pa.GetMinTimestamp()),
		MaxTimestamp:   Timestamp(pa.GetMaxTimestamp()),
		RequestID:      RequestID(pa.GetRequestID()),
	}
}

func ToProtoActionFetchRequest(a ActionFetchRequest) ProtoActionFetchRequest {
	return ProtoActionFetchRequest{

		MinActionID:    uint64(a.MinActionID),
		MaxActionID:    uint64(a.MaxActionID),
		ActorID:        uint64(a.ActorID),
		ActorType:      uint32(a.ActorType),
		TargetID:       uint64(a.TargetID),
		TargetType:     uint32(a.TargetType),
		ActionType:     uint32(a.ActionType),
		MinActionValue: a.MinActionValue,
		MaxActionValue: a.MaxActionValue,
		MinTimestamp:   uint64(a.MinTimestamp),
		MaxTimestamp:   uint64(a.MaxTimestamp),
		RequestID:      uint64(a.RequestID),
	}
}

func FromProtoActionList(actionList *ProtoActionList) []Action {
	actions := make([]Action, len(actionList.Actions))
	for i, pa := range actionList.Actions {
		actions[i] = FromProtoAction(pa)
	}
	return actions
}

func ToProtoActionList(actions []Action) *ProtoActionList {
	ret := &ProtoActionList{}
	ret.Actions = make([]*ProtoAction, len(actions))
	for i, action := range actions {
		pa := ToProtoAction(action)
		ret.Actions[i] = &pa
	}
	return ret
}

// Validate validates that all fields (except action ID) are appropriately specified
func (a *Action) Validate() error {
	if a.ActorID == 0 {
		return fmt.Errorf("actor ID can not be zero")
	}
	if a.ActorType == 0 {
		return fmt.Errorf("actor type can not be zero")
	}
	if a.TargetID == 0 {
		return fmt.Errorf("target ID can not be zero")
	}
	if a.TargetType == 0 {
		return fmt.Errorf("target type can not be zero")
	}
	if a.ActionType == 0 {
		return fmt.Errorf("action type can not be zero")
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
	if a.ActionValue != other.ActionValue {
		return false
	}
	if a.Timestamp != other.Timestamp {
		return false
	}
	if a.RequestID != other.RequestID {
		return false
	}
	return true
}
