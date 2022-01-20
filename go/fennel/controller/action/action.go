package action

import (
	"fennel/instance"
	actionlib "fennel/lib/action"
	"fennel/model/action"
	"fmt"
)

func Insert(this instance.Instance, a actionlib.Action) (uint64, error) {
	err := a.Validate()
	if err != nil {
		return 0, fmt.Errorf("can not insert action: %v", err)
	}
	return action.Insert(this, a)
}

func Fetch(this instance.Instance, request actionlib.ActionFetchRequest) ([]actionlib.Action, error) {
	return action.Fetch(this, request)
}
