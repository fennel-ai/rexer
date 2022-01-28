package action

import (
	"fennel/instance"
	"fennel/lib/action"
	"fmt"
	"strings"
)

// inserts the action. If successful, returns the actionID
func Insert(this instance.Instance, action action.Action) (uint64, error) {
	result, err := this.DB.NamedExec(`
		INSERT INTO actionlog (
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
		);`, action)
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
func Fetch(this instance.Instance, request action.ActionFetchRequest) ([]action.Action, error) {
	query := "SELECT * FROM actionlog"
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
