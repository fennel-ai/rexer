package usage

import (
	"context"
	"fennel/kafka"
	"fennel/lib/timer"
	usagelib "fennel/lib/usage"
	"fennel/model/usage"
	"fennel/tier"
	"time"

	"google.golang.org/protobuf/proto"
)

// Following Methods and helpers are for folding timestamp to the previous nearest
// hour or day.
// We use the FoldingStrategy during kafka read to fold and aggregate counters
// to the nearest previous hour or day.
// usage current is supported at an hour granularity.
// =============================================================
type FoldingStrategy func(uint64) uint64

func HourlyFold(t uint64) uint64 {
	div := time.Hour / time.Second
	return t - t%uint64(div)
}

func NoFold(t uint64) uint64 {
	return t
}

func DailyFold(t uint64) uint64 {
	div := 24 * (time.Hour / time.Second)
	return t - t%uint64(div)
}

func HourInSeconds() uint64 {
	return uint64(time.Hour / time.Second)
}

func DayInSeconds() uint64 {
	return 24 * HourInSeconds()
}

// =================================================================

func Insert(ctx context.Context, tier tier.Tier, b *usagelib.UsageCountersDBItem) error {
	ctx, t := timer.Start(ctx, tier.ID, "controller.usage.insert")
	defer t.Stop()
	bp := usagelib.ToUsageCountersProto(b)
	if bp.Timestamp == 0 {
		bp.Timestamp = uint64(tier.Clock.Now())
	}

	producer := tier.Producers[usagelib.HOURLY_USAGE_LOG_KAFKA_TOPIC]
	return producer.LogProto(ctx, bp, nil)
}

func InsertBatch(ctx context.Context, tier tier.Tier, bc []*usagelib.UsageCountersDBItem) error {
	ctx, t := timer.Start(ctx, tier.ID, "controller.usage.insert")
	defer t.Stop()
	producer := tier.Producers[usagelib.HOURLY_USAGE_LOG_KAFKA_TOPIC]
	for _, b := range bc {
		bp := usagelib.ToUsageCountersProto(b)
		if bp.Timestamp == 0 {
			bp.Timestamp = uint64(tier.Clock.Now())
		}
		if err := producer.LogProto(ctx, bp, nil); err != nil {
			return err
		}
	}
	return nil
}

func Read(ctx context.Context, consumer kafka.FConsumer, timeout time.Duration, foldingStrategy FoldingStrategy) (*usagelib.UsageCountersDBItem, error) {
	var ret usagelib.UsageCountersProto
	msg, err := consumer.Read(ctx, timeout)
	if err != nil {
		return nil, err
	}

	if err := proto.Unmarshal(msg, &ret); err != nil {
		return nil, err
	}
	return &usagelib.UsageCountersDBItem{
		Queries:   ret.Queries,
		Actions:   ret.Actions,
		Timestamp: foldingStrategy(ret.Timestamp),
	}, nil

}

func ReadBatch(ctx context.Context, consumer kafka.FConsumer, count int, timeout time.Duration, foldingStrategy FoldingStrategy) ([]*usagelib.UsageCountersDBItem, error) {
	msgs, err := consumer.ReadBatch(ctx, count, timeout)
	if err != nil {
		return nil, err
	}
	afterFold := make(map[uint64]*usagelib.UsageCountersDBItem)
	usageCounters := make([]*usagelib.UsageCountersDBItem, 0, len(msgs))
	for i := range msgs {
		var bc usagelib.UsageCountersProto
		if err = proto.Unmarshal(msgs[i], &bc); err != nil {
			return nil, err
		}
		old, ok := afterFold[foldingStrategy(bc.Timestamp)]
		if !ok {
			b := usagelib.FromUsageCountersProto(&bc)
			b.Timestamp = foldingStrategy(b.Timestamp)
			afterFold[foldingStrategy(bc.Timestamp)] = b
		} else {
			old.Queries += bc.Queries
			old.Actions += bc.Actions
		}
	}
	for _, v := range afterFold {
		usageCounters = append(usageCounters, v)
	}
	return usageCounters, nil
}

func TransferToDB(ctx context.Context, tr tier.Tier, consumer kafka.FConsumer, foldingStrategy FoldingStrategy) error {
	bc, err := ReadBatch(ctx, consumer, 1000, time.Second*10, foldingStrategy)
	if err != nil {
		return err
	}
	if len(bc) == 0 {
		return nil
	}
	if err = usage.InsertUsageCounters(ctx, tr, bc); err != nil {
		return err
	}
	_, err = consumer.Commit()
	return err
}
