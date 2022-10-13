package usage

import (
	"context"
	"log"
	"sort"
	"time"

	"fennel/kafka"
	"fennel/lib/timer"
	usagelib "fennel/lib/usage"
	"fennel/model/usage"
	"fennel/tier"

	"google.golang.org/protobuf/proto"
)

type UsageController interface {
	IncCounter(*usagelib.UsageCountersProto)
}

type controller struct {
	inputCh chan *usagelib.UsageCountersProto
	folder  usagelib.CounterOperator
	tier    *tier.Tier
	ctx     context.Context
}

func (c *controller) IncCounter(u *usagelib.UsageCountersProto) {
	if u.Timestamp == 0 {
		u.Timestamp = uint64(c.tier.Clock.Now().Unix())
	}
	c.inputCh <- u
}

func (c *controller) run() {
	for v := range c.folder.Output() {
		if err := c.insert(v); err != nil {
			log.Printf("Failed to insert to kafka: %s", err)
		}
	}
}

func NewController(ctx context.Context, tier *tier.Tier, foldThresholdDuration time.Duration, inputBufferSize, outputBufferSize, foldThresholdCounter int) UsageController {
	ch := make(chan *usagelib.UsageCountersProto, inputBufferSize)
	c := &controller{
		inputCh: ch,
		folder:  usagelib.NewFoldingOperator(ctx, ch, usagelib.HourlyFold, outputBufferSize, foldThresholdDuration, foldThresholdCounter),
		tier:    tier,
		ctx:     ctx,
	}
	go c.run()
	return c
}

func (c *controller) insert(b *usagelib.UsageCountersProto) error {
	ctx, t := timer.Start(c.ctx, c.tier.ID, "controller.usage.insert")
	defer t.Stop()
	producer := c.tier.Producers[usagelib.HOURLY_USAGE_LOG_KAFKA_TOPIC]
	return producer.LogProto(ctx, b, nil)
}

func ReadBatch(ctx context.Context, consumer kafka.FConsumer, count int, timeout time.Duration, foldingStrategy usagelib.FoldingStrategy) ([]*usagelib.UsageCountersProto, error) {
	msgs, err := consumer.ReadBatch(ctx, count, timeout)
	if err != nil {
		return nil, err
	}
	afterFold := make(map[uint64]*usagelib.UsageCountersProto)
	for i := range msgs {
		var bc usagelib.UsageCountersProto
		if err = proto.Unmarshal(msgs[i], &bc); err != nil {
			return nil, err
		}
		v, ok := afterFold[foldingStrategy(bc.Timestamp)]
		if ok {
			v.Queries += bc.Queries
			v.Actions += bc.Actions
		} else {
			bc.Timestamp = foldingStrategy(bc.Timestamp)
			afterFold[bc.Timestamp] = &bc
		}
	}
	ret := make([]*usagelib.UsageCountersProto, 0, len(afterFold))
	for _, v := range afterFold {
		ret = append(ret, v)
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Timestamp < ret[j].Timestamp
	})
	return ret, nil
}

func TransferToDB(ctx context.Context, consumer kafka.FConsumer, tr tier.Tier, foldingStrategy usagelib.FoldingStrategy, count int, timeout time.Duration) error {
	bc, err := ReadBatch(ctx, consumer, count, timeout, foldingStrategy)
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
