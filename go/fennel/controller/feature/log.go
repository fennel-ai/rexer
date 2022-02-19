package feature

import (
	"context"

	"fennel/lib/feature"
	"fennel/tier"
)

const (
	consumerGroup = "default"
)

func LogMulti(ctx context.Context, tr tier.Tier, rows []feature.Row) error {
	producer := tr.Producers[feature.KAFKA_TOPIC_NAME]
	for i := range rows {
		msg, err := feature.ToProto(rows[i])
		if err != nil {
			return err
		}
		if err = producer.LogProto(msg, nil); err != nil {
			return err
		}
	}
	return nil
}

func Log(ctx context.Context, tr tier.Tier, row feature.Row) error {
	return LogMulti(ctx, tr, []feature.Row{row})
}

func Read(ctx context.Context, tr tier.Tier) (*feature.Row, error) {
	consumer, err := tr.NewKafkaConsumer(feature.KAFKA_TOPIC_NAME, consumerGroup, "earliest")
	if err != nil {
		return nil, err
	}
	var prow feature.ProtoRow
	if err := consumer.ReadProto(&prow, -1); err != nil {
		return nil, err
	}
	row, err := feature.FromProtoRow(prow)
	if err != nil {
		return nil, err
	}
	return row, nil
}
