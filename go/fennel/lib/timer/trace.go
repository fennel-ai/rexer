package timer

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

type traceKey struct{}

type traceEvent struct {
	event   string
	elapsed time.Duration
}

type trace struct {
	lock   sync.Mutex
	start  time.Time
	events []traceEvent
}

func (t *trace) record(key string, ts time.Time) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.events = append(t.events, traceEvent{
		event:   key,
		elapsed: ts.Sub(t.start),
	})
}

func WithTracing(ctx context.Context) context.Context {
	return context.WithValue(ctx, traceKey{}, &trace{
		lock:   sync.Mutex{},
		start:  time.Now(),
		events: make([]traceEvent, 0),
	})
}

func LogTracingInfo(ctx context.Context, log *zap.Logger) error {
	ctxval := ctx.Value(traceKey{})
	if ctxval == nil {
		return nil
	}
	trace, ok := ctxval.(*trace)
	if !ok {
		return fmt.Errorf("expected trace but got: %v", ctxval)
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("====Trace====\n"))
	sort.Slice(trace.events, func(i, j int) bool {
		return trace.events[i].elapsed < trace.events[j].elapsed
	})
	for _, e := range trace.events {
		sb.WriteString(fmt.Sprintf("\t%5dms: %s\n", e.elapsed.Milliseconds(), e.event))
	}
	log.Debug(sb.String())
	return nil
}
