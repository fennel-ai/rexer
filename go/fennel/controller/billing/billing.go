package billing

import (
	"context"
	"fennel/kafka"
	billinglib "fennel/lib/billing"
	"fennel/lib/timer"
	"fennel/model/billing"
	"fennel/tier"
	"time"

	"google.golang.org/protobuf/proto"
)

// Following Methods and helpers are for folding timestamp to the previous nearest
// hour or day.
// We use the FoldingStrategy during kafka read to fold and aggregate counters
// to the nearest previous hour or day.
// Billing current is supported at an hour granularity.
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

func Insert(ctx context.Context, tier tier.Tier, b *billinglib.BillingCountersDBItem) error {
	ctx, t := timer.Start(ctx, tier.ID, "controller.billing.insert")
	defer t.Stop()
	bp := billinglib.ToBillingCountersProto(b)
	if bp.Timestamp == 0 {
		bp.Timestamp = uint64(tier.Clock.Now())
	}

	producer := tier.Producers[billinglib.HOURLY_BILLING_LOG_KAFKA_TOPIC]
	return producer.LogProto(ctx, bp, nil)
}

func InsertBatch(ctx context.Context, tier tier.Tier, bc []*billinglib.BillingCountersDBItem) error {
	ctx, t := timer.Start(ctx, tier.ID, "controller.billing.insert")
	defer t.Stop()
	producer := tier.Producers[billinglib.HOURLY_BILLING_LOG_KAFKA_TOPIC]
	for _, b := range bc {
		bp := billinglib.ToBillingCountersProto(b)
		if bp.Timestamp == 0 {
			bp.Timestamp = uint64(tier.Clock.Now())
		}
		if err := producer.LogProto(ctx, bp, nil); err != nil {
			return err
		}
	}
	return nil
}

func Read(ctx context.Context, consumer kafka.FConsumer, timeout time.Duration, foldingStrategy FoldingStrategy) (*billinglib.BillingCountersDBItem, error) {
	var ret billinglib.BillingCountersProto
	msg, err := consumer.Read(ctx, timeout)
	if err != nil {
		return nil, err
	}

	if err := proto.Unmarshal(msg, &ret); err != nil {
		return nil, err
	}
	return &billinglib.BillingCountersDBItem{
		Queries:   ret.Queries,
		Actions:   ret.Actions,
		Timestamp: foldingStrategy(ret.Timestamp),
	}, nil

}

func ReadBatch(ctx context.Context, consumer kafka.FConsumer, count int, timeout time.Duration, foldingStrategy FoldingStrategy) ([]*billinglib.BillingCountersDBItem, error) {
	msgs, err := consumer.ReadBatch(ctx, count, timeout)
	if err != nil {
		return nil, err
	}
	afterFold := make(map[uint64]*billinglib.BillingCountersDBItem)
	billingCounters := make([]*billinglib.BillingCountersDBItem, 0, len(msgs))
	for i := range msgs {
		var bc billinglib.BillingCountersProto
		if err = proto.Unmarshal(msgs[i], &bc); err != nil {
			return nil, err
		}
		old, ok := afterFold[foldingStrategy(bc.Timestamp)]
		if !ok {
			b := billinglib.FromBillingCountersProto(&bc)
			b.Timestamp = foldingStrategy(b.Timestamp)
			afterFold[foldingStrategy(bc.Timestamp)] = b
		} else {
			old.Queries += bc.Queries
			old.Actions += bc.Actions
		}
	}
	for _, v := range afterFold {
		billingCounters = append(billingCounters, v)
	}
	return billingCounters, nil
}

func TransferToDB(ctx context.Context, tr tier.Tier, consumer kafka.FConsumer, foldingStrategy FoldingStrategy) error {
	bc, err := ReadBatch(ctx, consumer, 1000, time.Second*10, foldingStrategy)
	if err != nil {
		return err
	}
	if len(bc) == 0 {
		return nil
	}
	if err = billing.InsertBillingCounters(ctx, tr, bc); err != nil {
		return err
	}
	_, err = consumer.Commit()
	return err
}
