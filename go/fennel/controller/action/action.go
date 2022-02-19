package action

import (
	"context"
	"fmt"

	actionlib "fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/model/action"
	"fennel/tier"
)

// Insert takes an action and inserts it both in the DB and Kafka
// returns the unique ID of the action that was inserted
func Insert(ctx context.Context, tier tier.Tier, a actionlib.Action) (uint64, error) {
	defer timer.Start(tier.ID, "controller.action.insert").ObserveDuration()
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
	ret, err := action.Insert(ctx, tier, a_ser)
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
	err = producer.LogProto(ctx, &pa, nil)
	if err != nil {
		return ret, err
	}
	return ret, nil
}

func Fetch(ctx context.Context, this tier.Tier, request actionlib.ActionFetchRequest) ([]actionlib.Action, error) {
	defer timer.Start(this.ID, "controller.action.fetch").ObserveDuration()
	actionsSer, err := action.Fetch(ctx, this, request)
	if err != nil {
		return nil, err
	}

	actions, err := actionlib.FromActionSerList(actionsSer)
	if err != nil {
		return nil, err
	}
	return actions, nil
}
