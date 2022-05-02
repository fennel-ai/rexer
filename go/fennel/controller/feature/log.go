package feature

import (
	"context"
	"time"

	"fennel/kafka"
	"fennel/lib/feature"
	"fennel/tier"
)

const (
	consumerGroup = "default"
)

func LogMulti(ctx context.Context, tr tier.Tier, rows []feature.Row) error {
	producer := tr.Producers[feature.KAFKA_TOPIC_NAME]
	for _, row := range rows {
		msg, err := row.MarshalJSON()
		if err != nil {
			return err
		}
		if err = producer.Log(ctx, msg, nil); err != nil {
			return err
		}
	}
	return producer.Flush(1 * time.Second)
}

func Log(ctx context.Context, tr tier.Tier, row feature.Row) error {
	return LogMulti(ctx, tr, []feature.Row{row})
}

func Read(ctx context.Context, consumer kafka.FConsumer) (*feature.Row, error) {
	data, err := consumer.Read(ctx, -1)
	if err != nil {
		return nil, err
	}
	var row feature.Row
	err = row.UnmarshalJSON(data)
	if err != nil {
		return nil, err
	}
	return &row, nil
}
