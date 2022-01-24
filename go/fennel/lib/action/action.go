package action

import (
	"fennel/lib/ftypes"
	"fmt"
)

const (
	ACTIONLOG_KAFKA_TOPIC = "actionlog"
)

const (
	Like  ftypes.ActionType = 1
	Share                   = 2
	View                    = 3
)

type Action struct {
	ActionID    ftypes.OidType    `db:"action_id"`
	ActorID     ftypes.OidType    `db:"actor_id"`
	ActorType   ftypes.OType      `db:"actor_type"`
	TargetID    ftypes.OidType    `db:"target_id"`
	TargetType  ftypes.OType      `db:"target_type"`
	ActionType  ftypes.ActionType `db:"action_type"`
	ActionValue int32             `db:"action_value"`
	Timestamp   ftypes.Timestamp  `db:"timestamp"`
	RequestID   ftypes.RequestID  `db:"request_id"`
	CustID      ftypes.CustID     `db:"cust_id"`
}

func FromProtoAction(pa *ProtoAction) Action {
	return Action{
		ActionID:    ftypes.OidType(pa.GetActionID()),
		ActorID:     ftypes.OidType(pa.GetActorID()),
		ActorType:   ftypes.OType(pa.GetActorType()),
		TargetID:    ftypes.OidType(pa.GetTargetID()),
		TargetType:  ftypes.OType(pa.GetTargetType()),
		ActionType:  ftypes.ActionType(pa.GetActionType()),
		ActionValue: pa.GetActionValue(),
		Timestamp:   ftypes.Timestamp(pa.GetTimestamp()),
		RequestID:   ftypes.RequestID(pa.RequestID),
		CustID:      ftypes.CustID(pa.CustID),
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
		CustID:      uint64(a.CustID),
	}
}

type ActionFetchRequest struct {
	MinActionID    ftypes.OidType    `db:"min_action_id"`
	MaxActionID    ftypes.OidType    `db:"max_action_id"`
	ActorID        ftypes.OidType    `db:"actor_id"`
	ActorType      ftypes.OType      `db:"actor_type"`
	TargetID       ftypes.OidType    `db:"target_id"`
	TargetType     ftypes.OType      `db:"target_type"`
	ActionType     ftypes.ActionType `db:"action_type"`
	MinActionValue int32             `db:"min_action_value"`
	MaxActionValue int32             `db:"max_action_value"`
	MinTimestamp   ftypes.Timestamp  `db:"min_timestamp"`
	MaxTimestamp   ftypes.Timestamp  `db:"max_timestamp"`
	RequestID      ftypes.RequestID  `db:"request_id"`
	CustID         ftypes.CustID     `db:"cust_id"`
}

func FromProtoActionFetchRequest(pa *ProtoActionFetchRequest) ActionFetchRequest {
	return ActionFetchRequest{

		MinActionID:    ftypes.OidType(pa.GetMinActionID()),
		MaxActionID:    ftypes.OidType(pa.GetMaxActionID()),
		ActorID:        ftypes.OidType(pa.GetActorID()),
		ActorType:      ftypes.OType(pa.GetActorType()),
		TargetID:       ftypes.OidType(pa.GetTargetID()),
		TargetType:     ftypes.OType(pa.GetTargetType()),
		ActionType:     ftypes.ActionType(pa.GetActionType()),
		MinActionValue: pa.GetMinActionValue(),
		MaxActionValue: pa.GetMaxActionValue(),
		MinTimestamp:   ftypes.Timestamp(pa.GetMinTimestamp()),
		MaxTimestamp:   ftypes.Timestamp(pa.GetMaxTimestamp()),
		RequestID:      ftypes.RequestID(pa.GetRequestID()),
		CustID:         ftypes.CustID(pa.GetCustID()),
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
		CustID:         uint64(a.CustID),
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
	if a.CustID == 0 {
		return fmt.Errorf("customer ID can not be zero")
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
	if a.CustID != other.CustID {
		return false
	}
	return true
}
