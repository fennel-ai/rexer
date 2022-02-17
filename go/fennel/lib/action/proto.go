package action

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

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
