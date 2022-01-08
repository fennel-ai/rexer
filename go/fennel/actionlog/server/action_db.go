package main

import (
	"fennel/actionlog/lib"
	"fennel/db"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"strings"
)

const (
	ACTION_LOG_TABLENAME = "actionlog"
)

func createActionTable() error {
	sql := fmt.Sprintf(`CREATE TABLE %s (
    	"action_id" integer not null primary key autoincrement,
		"actor_id" integer NOT NULL,
		"actor_type" integer NOT NULL,
		"target_id" integer NOT NULL,
		"target_type" integer NOT NULL,
		"action_type" integer NOT NULL,
		"action_value" integer NOT NULL,
		"timestamp" integer NOT NULL,
		"request_id" integer not null
	  );`, ACTION_LOG_TABLENAME)

	//log.Println("Creating actionlog table...", sql)
	statement, err := db.DB.Prepare(sql)
	if err != nil {
		return err
	}
	statement.Exec()
	log.Printf("'%s' table created\n", ACTION_LOG_TABLENAME)
	return nil
}

// inserts the action. If successful, returns the actionID
func actionDBInsert(action lib.Action) (uint64, error) {
	err := action.Validate()
	if err != nil {
		return 0, fmt.Errorf("can not insert action: %v", err)
	}
	//log.Printf("Inserting %v in table %s...\n", item, ACTION_LOG_TABLENAME)
	result, err := db.DB.NamedExec(fmt.Sprintf(`
		INSERT INTO %s (
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
			:actor_id,
			:actor_type,
			:target_id,
			:target_type,
			:action_type,
			:action_value,
			:timestamp,
			:request_id
		);`, ACTION_LOG_TABLENAME),
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
func actionDBGet(request lib.ActionFetchRequest) ([]lib.Action, error) {
	query := fmt.Sprintf("SELECT * FROM %s", ACTION_LOG_TABLENAME)
	clauses := make([]string, 0)
	if request.ActorType != 0 {
		clauses = append(clauses, "actor_type = :actor_type")
	}
	if request.ActorID != 0 {
		clauses = append(clauses, "actor_id = :actor_id")
	}
	if request.TargetType != 0 {
		clauses = append(clauses, "target_type = :target_type")
	}
	if request.TargetID != 0 {
		clauses = append(clauses, "target_id = :target_id")
	}
	if request.ActionType != 0 {
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
	//log.Printf("Action log db get query: %s\n", query)
	actions := make([]lib.Action, 0)
	statement, err := db.DB.PrepareNamed(query)
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

func actionDBPrintAll() error {
	// this is slow and will do full table scan. Just use it for debugging/dev
	var actions []lib.Action
	err := db.DB.Select(&actions, fmt.Sprintf("SELECT * FROM %s", ACTION_LOG_TABLENAME))
	if err != nil {
		return err
	}
	for _, item := range actions {
		fmt.Printf("%#v\n", item)
	}
	return nil
}
