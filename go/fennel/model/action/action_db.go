package action

import (
	"context"
	"fmt"
	"strings"

	"fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/value"
	"fennel/tier"
)

type actionSer struct {
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

// InsertBatch Inserts a batch of actions in the database and returns an error if there is a
// failure. All actions are inserted at once and so either the whole insertion works or none
// of it does
func InsertBatch(ctx context.Context, tier tier.Tier, actions []action.Action) error {
	ctx, t := timer.Start(ctx, tier.ID, "model.action.insert_batch")
	defer t.Stop()
	actionSers := make([]actionSer, len(actions))
	for i, a := range actions {
		if err := a.Validate(); err != nil {
			return fmt.Errorf("invalid action: %v", err)
		}
		if a.Timestamp == 0 {
			a.Timestamp = ftypes.Timestamp(tier.Clock.Now().Unix())
		}
		actionSers[i] = serializeAction(a)
	}
	if len(actions) == 0 {
		return nil
	}
	sql := `INSERT INTO actionlog (
				 actor_id, actor_type, target_id, target_type, action_type, timestamp, request_id, metadata
			)
			VALUES `
	var vals []interface{}
	for _, a := range actionSers {
		sql += "(?, ?, ?, ?, ?, ?, ?, ?),"
		vals = append(vals, a.ActorID, a.ActorType, a.TargetID, a.TargetType, a.ActionType, a.Timestamp, a.RequestID, a.Metadata)
	}
	sql = strings.TrimSuffix(sql, ",") // remove the last trailing comma
	_, err := tier.DB.ExecContext(ctx, sql, vals...)
	return err
}

// Whatever properties of 'request' are non-zero are used to filter eligible actions
// For actionID and timestamp ranges, min is exclusive and max is inclusive
// For actionValue range, both min/max are inclusive
// TODO: add limit support?
func Fetch(ctx context.Context, tier tier.Tier, request action.ActionFetchRequest) ([]action.Action, error) {
	ctx, t := timer.Start(ctx, tier.ID, "model.action.fetch")
	defer t.Stop()
	query := "SELECT * FROM actionlog"
	clauses := make([]string, 0)
	if len(request.ActorType) != 0 {
		clauses = append(clauses, "actor_type = :actor_type")
	}
	if len(request.ActorID) != 0 {
		clauses = append(clauses, "actor_id = :actor_id")
	}
	if len(request.TargetType) != 0 {
		clauses = append(clauses, "target_type = :target_type")
	}
	if len(request.TargetID) != 0 {
		clauses = append(clauses, "target_id = :target_id")
	}
	if len(request.ActionType) != 0 {
		clauses = append(clauses, "action_type = :action_type")
	}
	if request.MinTimestamp != 0 {
		clauses = append(clauses, "timestamp > :min_timestamp")
	}
	if request.MaxTimestamp != 0 {
		clauses = append(clauses, "timestamp <= :max_timestamp")
	}
	if len(request.RequestID) != 0 {
		clauses = append(clauses, "request_id = :request_id")
	}
	if request.MinActionID != 0 {
		clauses = append(clauses, "action_id > :min_action_id")
	}
	if request.MaxActionID != 0 {
		clauses = append(clauses, "action_id <= :max_action_id")
	}

	if len(clauses) > 0 {
		query = fmt.Sprintf("%s WHERE %s", query, strings.Join(clauses, " AND "))
	}
	query = fmt.Sprintf("%s ORDER BY timestamp DESC LIMIT 1000;", query)
	actions := make([]actionSer, 0)
	statement, err := tier.DB.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, err
	}
	err = statement.Select(&actions, request)
	if err != nil {
		return nil, err
	} else {
		actions, err := deserialize(actions...)
		if err != nil {
			return nil, err
		}
		return actions, nil
	}
}

func serializeAction(a action.Action) actionSer {
	return actionSer{
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

func deserialize(alSer ...actionSer) ([]action.Action, error) {
	al := make([]action.Action, len(alSer))
	for i, ser := range alSer {
		a := action.Action{
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
		if err != nil {
			return nil, err
		}
		al[i] = a
	}
	return al, nil
}
