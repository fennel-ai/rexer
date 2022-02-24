package action

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"fennel/kafka"
	actionlib "fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/model/action"
	"fennel/tier"
)

func dbInsert(ctx context.Context, tier tier.Tier, actions []actionlib.Action) error {
	actionSers := make([]actionlib.ActionSer, len(actions))
	for i, a := range actions {
		if err := a.Validate(); err != nil {
			return fmt.Errorf("invalid action: %v", err)
		}
		if a.Timestamp == 0 {
			a.Timestamp = ftypes.Timestamp(tier.Clock.Now())
		}
		aSer, err := a.ToActionSer()
		if err != nil {
			return err
		}
		actionSers[i] = *aSer
	}
	return action.InsertBatch(ctx, tier, actionSers)
}

// Insert takes an action and inserts it both in the DB and Kafka
// returns the unique ID of the action that was inserted
func Insert(ctx context.Context, tier tier.Tier, a actionlib.Action) error {
	defer timer.Start(ctx, tier.ID, "controller.action.insert").Stop()
	err := a.Validate()
	if err != nil {
		return fmt.Errorf("invalid action: %v", err)
	}
	if a.Timestamp == 0 {
		a.Timestamp = ftypes.Timestamp(tier.Clock.Now())
	}
	pa, err := actionlib.ToProtoAction(a)
	if err != nil {
		return err
	}
	producer := tier.Producers[actionlib.ACTIONLOG_KAFKA_TOPIC]
	return producer.LogProto(ctx, &pa, nil)
}

func Fetch(ctx context.Context, this tier.Tier, request actionlib.ActionFetchRequest) ([]actionlib.Action, error) {
	defer timer.Start(ctx, this.ID, "controller.action.fetch").Stop()
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

func ReadBatch(ctx context.Context, consumer kafka.FConsumer, count int, timeout time.Duration) ([]actionlib.Action, error) {
	msgs, err := consumer.ReadBatch(ctx, count, timeout)
	if err != nil {
		return nil, err
	}
	actions := make([]actionlib.Action, len(msgs))
	for i := range msgs {
		var pa actionlib.ProtoAction
		if err = proto.Unmarshal(msgs[i], &pa); err != nil {
			return nil, err
		}
		if actions[i], err = actionlib.FromProtoAction(&pa); err != nil {
			return nil, err
		}
	}
	return actions, nil
}

func TransferToDB(ctx context.Context, tr tier.Tier, consumer kafka.FConsumer) error {
	actions, err := ReadBatch(ctx, consumer, 1000, time.Second*5)
	if err == nil {
		err = dbInsert(ctx, tr, actions)
	}
	return err
}
