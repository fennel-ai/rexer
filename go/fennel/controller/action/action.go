package action

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"fennel/kafka"
	actionlib "fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/model/action"
	"fennel/tier"
)

var jsonLogErrs = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "actions_json_log_err",
		Help: "Total number of failures to log actions as JSON.",
	},
	[]string{"err"},
)

func Insert(ctx context.Context, tier tier.Tier, a actionlib.Action) error {
	ctx, t := timer.Start(ctx, tier.ID, "controller.action.insert")
	defer t.Stop()
	if a.Timestamp == 0 {
		a.Timestamp = ftypes.Timestamp(tier.Clock.Now().Unix())
	}
	pa, err := actionlib.ToProtoAction(a)
	if err != nil {
		return err
	}

	// Best-effort write for actions in JSON format.
	jsonProducer := tier.Producers[actionlib.ACTIONLOG_JSON_KAFKA_TOPIC]
	// Log actions as JSON in a best-effort way - this is used for joining
	// actions with feature logs to create training data, but failure
	// to log as JSON should not fail the request.
	j, err := a.MarshalJSON()
	if err != nil {
		tier.Logger.Warn("could not marshal action into JSON: ", zap.Error(err))
		jsonLogErrs.WithLabelValues("marshal").Inc()
	}
	if err = jsonProducer.Log(ctx, j, nil); err != nil {
		tier.Logger.Warn("could not log JSON action: ", zap.Error(err))
		jsonLogErrs.WithLabelValues("log").Inc()
	}

	producer := tier.Producers[actionlib.ACTIONLOG_KAFKA_TOPIC]
	return producer.LogProto(ctx, &pa, nil)
}

func BatchInsert(ctx context.Context, tier tier.Tier, actions []actionlib.Action) error {
	ctx, t := timer.Start(ctx, tier.ID, "controller.action.batchinsert")
	defer t.Stop()
	// validate all the actions first so that there are no partial entries due to invalid inputs.
	protos := make([]*actionlib.ProtoAction, 0, len(actions))
	jsons := make([][]byte, 0, len(actions))
	for _, a := range actions {
		err := a.Validate()
		if err != nil {
			return fmt.Errorf("invalid action: %v", err)
		}
		if a.Timestamp == 0 {
			a.Timestamp = ftypes.Timestamp(tier.Clock.Now().Unix())
		}
		pa, err := actionlib.ToProtoAction(a)
		if err != nil {
			return err
		}
		protos = append(protos, &pa)
		j, err := a.MarshalJSON()
		if err != nil {
			tier.Logger.Warn("could not marshal action into JSON: ", zap.Error(err))
			jsonLogErrs.WithLabelValues("marshal").Inc()
			continue
		}
		jsons = append(jsons, j)
	}

	// best effort write for actions in JSON format
	go func() {
		jsonProducer := tier.Producers[actionlib.ACTIONLOG_JSON_KAFKA_TOPIC]
		for _, j := range jsons {
			if err := jsonProducer.Log(ctx, j, nil); err != nil {
				tier.Logger.Warn("could not log JSON action: ", zap.Error(err))
				jsonLogErrs.WithLabelValues("log").Inc()
			}
		}
	}()

	producer := tier.Producers[actionlib.ACTIONLOG_KAFKA_TOPIC]
	// TODO: Define and implement batch logging once the downstream API moves out of experimental
	for _, p := range protos {
		err := producer.LogProto(ctx, p, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func Fetch(ctx context.Context, this tier.Tier, request actionlib.ActionFetchRequest) ([]actionlib.Action, error) {
	ctx, t := timer.Start(ctx, this.ID, "controller.action.fetch")
	defer t.Stop()
	return action.Fetch(ctx, this, request)
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
	actions, err := ReadBatch(ctx, consumer, 950, time.Second*10)
	if err != nil {
		return err
	}
	if len(actions) == 0 {
		return nil
	}
	if err = action.InsertBatch(ctx, tr, actions); err != nil {
		return err
	}
	_, err = consumer.Commit()
	return err
}
