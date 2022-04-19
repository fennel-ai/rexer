package action

import (
	"context"
	"fmt"
	"strings"

	"fennel/lib/action"
	"fennel/lib/timer"
	"fennel/tier"
)

// Insert inserts the action in DB. If successful, returns the actionID
func Insert(ctx context.Context, tier tier.Tier, action *action.ActionSer) (uint64, error) {
	defer timer.Start(ctx, tier.ID, "model.action.insert").Stop()
	result, err := tier.DB.NamedExecContext(ctx, `
		INSERT INTO actionlog (
			actor_id, actor_type, target_id, target_type, action_type, timestamp, request_id, metadata
	    )
        VALUES (
			:actor_id, :actor_type, :target_id, :target_type, :action_type, :timestamp, :request_id, :metadata
		);`,
		action)
	if err != nil {
		return 0, err
	}
	actionID, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	return uint64(actionID), nil
}

// InsertBatch Inserts a batch of actions in the database and returns an error if there is a
// failure. All actions are inserted at once and so either the whole insertion works or none
// of it does
func InsertBatch(ctx context.Context, tier tier.Tier, actions []action.ActionSer) error {
	defer timer.Start(ctx, tier.ID, "model.action.insert_batch").Stop()
	if len(actions) == 0 {
		return nil
	}
	sql := `INSERT INTO actionlog (
				 actor_id, actor_type, target_id, target_type, action_type, timestamp, request_id, metadata
			)
			VALUES `
	vals := make([]interface{}, 0)
	for i := range actions {
		a := actions[i]
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
func Fetch(ctx context.Context, tier tier.Tier, request action.ActionFetchRequest) ([]action.ActionSer, error) {
	defer timer.Start(ctx, tier.ID, "model.action.fetch").Stop()
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
	query = fmt.Sprintf("%s ORDER BY timestamp LIMIT 1000;", query)
	actions := make([]action.ActionSer, 0)
	statement, err := tier.DB.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, err
	}
	err = statement.Select(&actions, request)
	if err != nil {
		return nil, err
	} else {
		return actions, nil
	}
}
