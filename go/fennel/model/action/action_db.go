package action

import (
	"fennel/db"
	"fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/plane"
	"fmt"
	"strings"
)

func tablename(planeID ftypes.PlaneID) (string, error) {
	return db.ToPlaneTablename(planeID, "actionlog")
}

// inserts the action. If successful, returns the actionID
func Insert(this plane.Plane, action action.Action) (uint64, error) {
	if len(action.ActionType) > 256 {
		return 0, fmt.Errorf("ActionType too long: action types cannot be longer than 256 chars")
	}
	if len(action.ActorType) > 256 {
		return 0, fmt.Errorf("ActorType too long: actor types cannot be longer than 256 chars")
	}
	if len(action.TargetType) > 256 {
		return 0, fmt.Errorf("TargetType too long: target types cannot be longer than 256 chars")
	}
	table, err := tablename(this.ID)
	if err != nil {
		return 0, err
	}
	result, err := this.DB.NamedExec(fmt.Sprintf(`
		INSERT INTO %s (
			cust_id,
			actor_id,
			actor_type,
			target_id,
			target_type,
			action_type,
			action_value,
			timestamp,
			request_id
	    )
        VALUES (
			:cust_id,
			:actor_id,
			:actor_type,
			:target_id,
			:target_type,
			:action_type,
			:action_value,
			:timestamp,
			:request_id
		);`, table), action)
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
func Fetch(this plane.Plane, request action.ActionFetchRequest) ([]action.Action, error) {
	table, err := tablename(this.ID)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("SELECT * FROM %s", table)
	clauses := make([]string, 0)
	if request.CustID != 0 {
		clauses = append(clauses, "cust_id = :cust_id")
	}
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
	if request.MinActionValue != 0 {
		clauses = append(clauses, "action_value >= :min_action_value")
	}
	if request.MaxActionValue != 0 {
		clauses = append(clauses, "action_value <= :max_action_value")
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
	actions := make([]action.Action, 0)
	statement, err := this.DB.PrepareNamed(query)
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
