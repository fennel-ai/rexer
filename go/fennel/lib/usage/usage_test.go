package usage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHourlyFolding(t *testing.T) {
	ch := make(chan *UsageCountersProto)
	ctx, cancelFn := context.WithCancel(context.Background())
	folder := NewFoldingOperator(ctx, ch, HourlyFold, 50, time.Minute, 50)
	for i := 0; i < 50; i++ {
		ch <- &UsageCountersProto{
			Queries:   uint64(10),
			Actions:   uint64(20),
			Timestamp: uint64(i),
		}
	}
	for i := 0; i < 25; i++ {
		val := <-folder.Output()
		assert.Equal(t, uint64(20), val.Queries)
		assert.Equal(t, uint64(40), val.Actions)
		assert.Equal(t, uint64(0), val.Timestamp)
	}
	cancelFn()
}

func TestNoFolding(t *testing.T) {
	ch := make(chan *UsageCountersProto)
	ctx, cancelFn := context.WithCancel(context.Background())
	folder := NewFoldingOperator(ctx, ch, NoFold, 50, time.Minute, 50)
	for i := 0; i < 50; i++ {
		ch <- &UsageCountersProto{
			Queries:   uint64(i),
			Actions:   uint64(2 * i),
			Timestamp: uint64(i),
		}
	}
	for i := 0; i < 50; i++ {
		val := <-folder.Output()
		assert.Equal(t, uint64(i), val.Queries)
		assert.Equal(t, uint64(2*i), val.Actions)
		assert.Equal(t, uint64(i), val.Timestamp)
	}
	cancelFn()
}

func TestWithLessThanThreshold(t *testing.T) {
	ch := make(chan *UsageCountersProto)
	ctx, cancelFn := context.WithCancel(context.Background())
	folder := NewFoldingOperator(ctx, ch, HourlyFold, 50, time.Second, 50)
	for i := 0; i < 25; i++ {
		ch <- &UsageCountersProto{
			Queries:   uint64(i),
			Actions:   uint64(2 * i),
			Timestamp: uint64(i),
		}
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		queries, actions := uint64(0), uint64(0)
		for v := range folder.Output() {
			queries += v.Queries
			actions += v.Actions
		}
		assert.Equal(t, uint64(12*25), queries)
		assert.Equal(t, uint64(24*25), actions)
		wg.Done()
	}()
	cancelFn()
	wg.Wait()
}
