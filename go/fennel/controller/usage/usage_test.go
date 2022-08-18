package usage

import (
	"context"
	"fennel/kafka"
	"fennel/lib/usage"
	usagelib "fennel/lib/usage"
	"fennel/lib/utils"
	usagemodel "fennel/model/usage"
	"fennel/resource"
	"fennel/test"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInsertAndRead(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	timestamp := uint64(tier.Clock.Now())
	assert.NoError(t, Insert(ctx, tier, &usage.UsageCountersDBItem{Queries: 3, Actions: 4, Timestamp: timestamp}))
	consumer, err := tier.NewKafkaConsumer(
		kafka.ConsumerConfig{
			Scope:        resource.NewTierScope(tier.ID),
			Topic:        usagelib.HOURLY_USAGE_LOG_KAFKA_TOPIC,
			GroupID:      utils.RandString(6),
			OffsetPolicy: kafka.DefaultOffsetPolicy,
		},
	)
	assert.NoError(t, err)
	defer consumer.Close()
	usageCounter, err := Read(ctx, consumer, 5*time.Second, HourlyFold)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), usageCounter.Queries)
	assert.Equal(t, uint64(4), usageCounter.Actions)
	assert.Equal(t, HourlyFold(timestamp), usageCounter.Timestamp)

}

func TestInsertBatchAndReadBatchAndTransferToDB(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	usageCountersItems := make([]*usage.UsageCountersDBItem, 10)
	startTime := HourlyFold(uint64(tier.Clock.Now()))
	for i := 0; i < 10; i++ {
		usageCountersItems[i] = &usagelib.UsageCountersDBItem{
			Queries:   uint64(i),
			Actions:   uint64(i),
			Timestamp: startTime + (HourInSeconds()*uint64(i))/2,
		}
	}
	assert.NoError(t, InsertBatch(ctx, tier, usageCountersItems))
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		consumer, err := tier.NewKafkaConsumer(
			kafka.ConsumerConfig{
				Scope:        resource.NewTierScope(tier.ID),
				Topic:        usagelib.HOURLY_USAGE_LOG_KAFKA_TOPIC,
				GroupID:      utils.RandString(6),
				OffsetPolicy: kafka.DefaultOffsetPolicy,
			},
		)
		assert.NoError(t, err)
		defer consumer.Close()
		usageCountersRead, err := ReadBatch(ctx, consumer, 10, 5*time.Second, HourlyFold)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(usageCountersRead))
		sort.Slice(usageCountersRead, func(i, j int) bool {
			return usageCountersRead[i].Timestamp < usageCountersRead[j].Timestamp
		})
		for i := 0; i < 5; i++ {
			assert.Equal(t, uint64(4*i+1), usageCountersRead[i].Queries)
			assert.Equal(t, uint64(4*i+1), usageCountersRead[i].Actions)
			assert.Equal(t, startTime+(HourInSeconds()*uint64(i)), usageCountersRead[i].Timestamp)
		}
		wg.Done()
	}()
	go func() {
		consumer, err := tier.NewKafkaConsumer(
			kafka.ConsumerConfig{
				Scope:        resource.NewTierScope(tier.ID),
				Topic:        usagelib.HOURLY_USAGE_LOG_KAFKA_TOPIC,
				GroupID:      utils.RandString(6),
				OffsetPolicy: kafka.DefaultOffsetPolicy,
			},
		)
		assert.NoError(t, err)
		defer consumer.Close()
		assert.NoError(t, TransferToDB(ctx, tier, consumer, HourlyFold))
		previous := uint64(0)
		for i := 0; i < 5; i++ {
			current := uint64(4*i+1) + previous
			b, err := usagemodel.GetUsageCounters(ctx, tier, startTime, startTime+HourInSeconds()*uint64(i)+1)
			assert.NoError(t, err)
			assert.Equal(t, current, b.Queries)
			assert.Equal(t, current, b.Actions)
			assert.Equal(t, startTime, b.StartTime)
			assert.Equal(t, startTime+HourInSeconds()*uint64(i)+1, b.EndTime)
			previous = current
		}
		wg.Done()
	}()

}
