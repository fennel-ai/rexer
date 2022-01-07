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
	ActionValue int        `db:"action_value"`
	Timestamp   Timestamp  `db:"timestamp"`
	RequestID   RequestID  `db:"request_id"`
}

type ActionFetchRequest struct {
	MinActionID    OidType    `db:"min_action_id"`
	MaxActionID    OidType    `db:"max_action_id"`
	ActorID        OidType    `db:"actor_id"`
	ActorType      OType      `db:"actor_type"`
	TargetID       OidType    `db:"target_id"`
	TargetType     OType      `db:"target_type"`
	ActionType     ActionType `db:"action_type"`
	MinActionValue int        `db:"min_action_value"`
	MaxActionValue int        `db:"max_action_value"`
	MinTimestamp   Timestamp  `db:"min_timestamp"`
	MaxTimestamp   Timestamp  `db:"max_timestamp"`
	RequestID      RequestID  `db:"request_id"`
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
