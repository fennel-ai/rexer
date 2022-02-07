package action

import (
	"fennel/lib/action"
	"fennel/tier"
	"fmt"
	"strings"
)

// inserts the action. If successful, returns the actionID
func Insert(tier tier.Tier, action *action.ActionSer) (uint64, error) {
	if len(action.ActionType) > 256 {
		return 0, fmt.Errorf("ActionType too long: action types cannot be longer than 256 chars")
	}
	if len(action.ActorType) > 256 {
		return 0, fmt.Errorf("ActorType too long: actor types cannot be longer than 256 chars")
	}
	if len(action.TargetType) > 256 {
		return 0, fmt.Errorf("TargetType too long: target types cannot be longer than 256 chars")
	}
	result, err := tier.DB.NamedExec(`
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

// Whatever properties of 'request' are non-zero are used to filter eligible actions
// For actionID and timestamp ranges, min is exclusive and max is inclusive
// For actionValue range, both min/max are inclusive
// TODO: add limit support?
func Fetch(tier tier.Tier, request action.ActionFetchRequest) ([]action.ActionSer, error) {
	query := "SELECT * FROM actionlog"
	clauses := make([]string, 0)
	if len(request.ActorType) != 0 {
		clauses = append(clauses, "actor_type = :actor_type")
	}
	if request.ActorID != 0 {
		clauses = append(clauses, "actor_id = :actor_id")
	}
	if len(request.TargetType) != 0 {
		clauses = append(clauses, "target_type = :target_type")
	}
	if request.TargetID != 0 {
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
	if request.RequestID != 0 {
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
	query = fmt.Sprintf("%s ORDER BY timestamp;", query)
	actions := make([]action.ActionSer, 0)
	statement, err := tier.DB.PrepareNamed(query)
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
