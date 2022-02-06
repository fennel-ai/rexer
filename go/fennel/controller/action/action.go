package action

import (
	actionlib "fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/model/action"
	"fennel/tier"
	"fmt"
	"time"
)

// Insert takes an action and inserts it both in the DB and Kafka
// returns the unique ID of the action that was inserted
func Insert(tier tier.Tier, a actionlib.Action) (uint64, error) {
	if a.CustID == 0 {
		a.CustID = tier.CustID
	}
	err := a.Validate()
	if err != nil {
		return 0, fmt.Errorf("invalid action: %v", err)
	}
	if a.Timestamp == 0 {
		a.Timestamp = ftypes.Timestamp(time.Now().Unix())
	}
	ret, err := action.Insert(tier, a)
	if err != nil {
		return ret, err
	}
	pa := actionlib.ToProtoAction(a)
	producer := tier.Producers[actionlib.ACTIONLOG_KAFKA_TOPIC]
	err = producer.Log(&pa)
	if err != nil {
		return ret, err
	}
	return ret, nil
}

func Fetch(this tier.Tier, request actionlib.ActionFetchRequest) ([]actionlib.Action, error) {
	return action.Fetch(this, request)
}
