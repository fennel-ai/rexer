package usage

import (
	"context"
	"sort"
	"time"
)

const (
	HOURLY_USAGE_LOG_KAFKA_TOPIC = "hourly_usage_log"
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

type CounterOperator interface {
	Output() <-chan *UsageCountersProto
}

type foldingOperator struct {
	foldingStrategy      FoldingStrategy
	inputChan            <-chan *UsageCountersProto
	outputChan           chan *UsageCountersProto
	ctx                  context.Context
	ticker               *time.Ticker
	foldThresholdCounter int
}

func NewFoldingOperator(ctx context.Context, input <-chan *UsageCountersProto, foldingStrategy FoldingStrategy, outputChanBuffer int, foldThresholdDuration time.Duration, foldThresholdCounter int) CounterOperator {
	f := &foldingOperator{
		foldingStrategy:      foldingStrategy,
		inputChan:            input,
		outputChan:           make(chan *UsageCountersProto, outputChanBuffer),
		ctx:                  ctx,
		ticker:               time.NewTicker(foldThresholdDuration),
		foldThresholdCounter: foldThresholdCounter,
	}
	go f.run()
	return f
}

func (u *foldingOperator) Output() <-chan *UsageCountersProto {
	return u.outputChan
}

func (u *foldingOperator) run() {
	aggVal := make(map[uint64]*UsageCountersProto)
	counter := uint64(0)
	fold := func(val *UsageCountersProto) {
		v, ok := aggVal[u.foldingStrategy(val.Timestamp)]
		if ok {
			v.Queries += val.Queries
			v.Actions += val.Actions
		} else {
			val.Timestamp = u.foldingStrategy(val.Timestamp)
			aggVal[val.Timestamp] = val
		}
		counter += (val.Queries + val.Actions)
	}
	writeAll := func() {
		values := make([]*UsageCountersProto, len(aggVal))
		i := 0
		for k := range aggVal {
			values[i] = aggVal[k]
			i++
		}
		sort.Slice(values, func(i, j int) bool {
			return values[i].Timestamp < values[j].Timestamp
		})
		for _, v := range values {
			u.outputChan <- v
		}

		for k := range aggVal {
			delete(aggVal, k)
		}
		counter = 0
	}
loop:
	for {
		select {
		case <-u.ctx.Done():
			writeAll()
			close(u.outputChan)
			break loop
		case val := <-u.inputChan:
			fold(val)
			if counter >= uint64(u.foldThresholdCounter) {
				writeAll()
			}
		case <-u.ticker.C:
			writeAll()
		}
	}
}
