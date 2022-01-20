package action

import (
	"fennel/instance"
	actionlib "fennel/lib/action"
	"fennel/model/action"
	"fmt"
)

// Insert takes an action and inserts it both in the DB and Kafka
// returns the unique ID of the action that was inserted
func Insert(this instance.Instance, a actionlib.Action) (uint64, error) {
	err := a.Validate()
	if err != nil {
		return 0, fmt.Errorf("invalid action: %v", err)
	}
	ret, err := action.Insert(this, a)
	if err != nil {
		return ret, err
	}
	pa := actionlib.ToProtoAction(a)
	err = this.ActionProducer.Log(&pa)
	if err != nil {
		return ret, err
	}
	return ret, nil
}

func Fetch(this instance.Instance, request actionlib.ActionFetchRequest) ([]actionlib.Action, error) {
	return action.Fetch(this, request)
}
