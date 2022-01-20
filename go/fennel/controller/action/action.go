package action

import (
	"fennel/instance"
	actionlib "fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/model/action"
	"fmt"
	"time"
)

// Insert takes an action and inserts it both in the DB and Kafka
// returns the unique ID of the action that was inserted
func Insert(this instance.Instance, a actionlib.Action) (uint64, error) {
	err := a.Validate()
	if err != nil {
		return 0, fmt.Errorf("invalid action: %v", err)
	}
	if a.Timestamp == 0 {
		a.Timestamp = ftypes.Timestamp(time.Now().Unix())
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
