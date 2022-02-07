package action

import (
	actionlib "fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/model/action"
	"fennel/tier"
	"fmt"
)

// Insert takes an action and inserts it both in the DB and Kafka
// returns the unique ID of the action that was inserted
func Insert(tier tier.Tier, a actionlib.Action) (uint64, error) {
	err := a.Validate()
	if err != nil {
		return 0, fmt.Errorf("invalid action: %v", err)
	}
	if a.Timestamp == 0 {
		a.Timestamp = ftypes.Timestamp(tier.Clock.Now())
	}
	a_ser, err := a.ToActionSer()
	if err != nil {
		return 0, nil
	}
	ret, err := action.Insert(tier, a_ser)
	if err != nil {
		return ret, err
	}
	pa, err := actionlib.ToProtoAction(a)
	if err != nil {
		return ret, err
	}
	a.ActionID = ftypes.OidType(ret)
	pa, err = actionlib.ToProtoAction(a)
	if err != nil {
		return ret, err
	}
	producer := tier.Producers[actionlib.ACTIONLOG_KAFKA_TOPIC]
	err = producer.Log(&pa)
	if err != nil {
		return ret, err
	}
	return ret, nil
}

func Fetch(this tier.Tier, request actionlib.ActionFetchRequest) ([]actionlib.Action, error) {
	actionsSer, err := action.Fetch(this, request)
	if err != nil {
		return nil, err
	}

	actions, err := actionlib.FromActionSerList(actionsSer)
	if err != nil {
		return nil, err
	}
	return actions, nil
}
