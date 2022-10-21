package action

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/buger/jsonparser"
)

const (
	ACTIONLOG_KAFKA_TOPIC = "actionlog"
	// NOTE: `actionlog_json` is used to log actions as "labels" of the training dataset;
	ACTIONLOG_JSON_KAFKA_TOPIC = "actionlog_json"
)

var totalActionsFutureEventTs = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "future_action_events_total",
		Help: "Number of actions with event timestamp in the future.",
	},
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

func (a Action) ToValueDict() (value.Dict, error) {
	actorID, err := value.FromJSON([]byte(a.ActorID))
	if err != nil {
		return value.Dict{}, err
	}
	targetID, err := value.FromJSON([]byte(a.TargetID))
	if err != nil {
		return value.Dict{}, err
	}
	requestID, err := value.FromJSON([]byte(a.RequestID))
	if err != nil {
		return value.Dict{}, err
	}
	return value.NewDict(map[string]value.Value{
		"action_id":   value.Int(a.ActionID),
		"actor_id":    actorID,
		"actor_type":  value.String(a.ActorType),
		"target_type": value.String(a.TargetType),
		"target_id":   targetID,
		"action_type": value.String(a.ActionType),
		"timestamp":   value.Int(a.Timestamp),
		"request_id":  requestID,
		"metadata":    a.Metadata,
	}), nil
}

func acceptablePastTimestamp(tsNow, ts, multiplier int64) bool {
	// 2 years
	return tsNow - ts <= 63072000 * multiplier
}

func FromValueDict(dict value.Dict) (Action, error) {
	var action Action
	if actionID, ok := dict.Get("action_id"); ok {
		if aid, ok := actionID.(value.Int); ok {
			action.ActionID = ftypes.IDType(aid)
		} else {
			return action, fmt.Errorf("action ID must be an integer")
		}
	}

	if actorID, ok := dict.Get("actor_id"); ok {
		action.ActorID = ftypes.OidType(value.ToJSON(actorID))
	} else {
		return action, fmt.Errorf("action missing actor ID")
	}

	if actorType, ok := dict.Get("actor_type"); ok {
		if at, ok := actorType.(value.String); ok {
			action.ActorType = ftypes.OType(at)
		} else {
			return action, fmt.Errorf("actor type must be a string")
		}
	} else {
		return action, fmt.Errorf("action missing actor type")
	}

	if targetID, ok := dict.Get("target_id"); ok {
		action.TargetID = ftypes.OidType(value.ToJSON(targetID))
	} else {
		return action, fmt.Errorf("action missing target ID")
	}

	if targetType, ok := dict.Get("target_type"); ok {
		if t, ok := targetType.(value.String); ok {
			action.TargetType = ftypes.OType(t)
		} else {
			return action, fmt.Errorf("target type must be a string")
		}
	} else {
		return action, fmt.Errorf("action missing target type")
	}

	if actionType, ok := dict.Get("action_type"); ok {
		if at, ok := actionType.(value.String); ok {
			action.ActionType = ftypes.ActionType(at)
		} else {
			return action, fmt.Errorf("action type must be a string")
		}
	} else {
		return action, fmt.Errorf("action missing action type")
	}

	if timestamp, ok := dict.Get("timestamp"); ok {

		if ts, ok := timestamp.(value.Int); ok {
			// parse the given timestamp as unix seconds, it is possible that the given timestamp is milliseconds,
			// microseconds or nanoseconds

			// TODO(mohit): Remove this hack when we should standardize the expectations around the timestamp format
			now := time.Now()
			// Add buffer to avoid clock skew, 1 month
			now = now.Add(30 * 86400 * time.Second)
			ts64 := int64(ts)
			if ts64 > now.Unix() {
				// ts in seconds could be in future or milliseconds past
				if ts64 <= now.UnixMilli() && acceptablePastTimestamp(now.UnixMilli(), ts64, 1_000)  {
					// this is milliseconds mostly
					action.Timestamp = ftypes.Timestamp(ts64 / 1_000)
				} else if ts64 <= now.UnixMicro() && acceptablePastTimestamp(now.UnixMicro(), ts64, 1_000_000) {
					action.Timestamp = ftypes.Timestamp(ts64 / 1_000_000)
				} else if ts64 <= now.UnixNano() && acceptablePastTimestamp(now.UnixNano(), ts64, 1_000_000_000) {
					action.Timestamp = ftypes.Timestamp(ts64 / 1_000_000_000)
				} else {
					totalActionsFutureEventTs.Inc()
					return action, fmt.Errorf("timestamp is potentially from the future which is now allowed: %v, now unixSec: %v", ts64, now.Unix())
				}
			} else {
				// the timestamp is from the past which is allowed and is okay
				action.Timestamp = ftypes.Timestamp(ts)
			}
		} else if ts, ok := timestamp.(value.Double); ok {
			tsCast := int64(ts)
			if float64(tsCast)-float64(ts) > 0.0001 {
				return action, fmt.Errorf("timestamp must be an integer")
			}
			action.Timestamp = ftypes.Timestamp(tsCast)
		} else if ts, ok := timestamp.(value.String); ok {
			timeParsed, err := time.Parse(time.RFC3339, string(ts))
			if err != nil {
				timeParsed, err = time.Parse(time.RFC3339, string(ts+"Z"))
			}
			if err != nil {
				return action, fmt.Errorf("timestamp must be an integer or RFC3339 formatted string: %w", err)
			}
			action.Timestamp = ftypes.Timestamp(timeParsed.Unix())
		} else {
			return action, fmt.Errorf("action timestamp must be an integer")
		}
	} else {
		action.Timestamp = ftypes.Timestamp(time.Now().Unix())
	}

	if requestID, ok := dict.Get("request_id"); ok {
		action.RequestID = ftypes.RequestID(value.ToJSON(requestID))
	} else {
		return action, fmt.Errorf("action missing request ID")
	}

	if metadata, ok := dict.Get("metadata"); ok {
		action.Metadata = metadata
	}

	return action, nil
}

// ToList takes a list of actions and arranges that in a value.List
// else returns errors
func ToList(actions []Action) (value.List, error) {
	table := value.List{}
	table.Grow(len(actions))
	for i := range actions {
		d, err := actions[i].ToValueDict()
		if err != nil {
			return value.List{}, err
		}
		table.Append(d)
	}
	return table, nil
}

func ToJsonList(actions []Action) ([][]byte, error) {
	res := make([][]byte, len(actions))
	for i, action := range actions {
		actionJson, err := action.MarshalJSON()
		if err != nil {
			return nil, err
		}
		res[i] = actionJson
	}
	return res, nil
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
