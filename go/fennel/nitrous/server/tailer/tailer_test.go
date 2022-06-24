package tailer_test

import (
	"context"
	"testing"
	"time"

	"fennel/hangar"
	"fennel/lib/nitrous"
	"fennel/lib/utils/ptr"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server/offsets"
	"fennel/nitrous/server/tailer"
	"fennel/plane"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

type countingProcessor struct {
	t       *testing.T
	counter *atomic.Int32
}

func (c countingProcessor) Identity() string {
	return "countingProcessor"
}

func (c countingProcessor) Process(ctx context.Context, ops []*rpc.NitrousOp) (keys []hangar.Key, vgs []hangar.ValGroup, err error) {
	c.t.Log("got notified")
	_ = c.counter.Inc()
	return nil, nil, nil
}

var offsetkg = []byte("offsetkg")

func TestTailer(t *testing.T) {
	tp := plane.NewTestPlane(t)
	p := tp.Plane
	tlr, err := tailer.NewTailer(p, nitrous.BINLOG_KAFKA_TOPIC, nil, offsetkg)
	assert.NoError(t, err)
	notifs := atomic.NewInt32(0)
	p1 := countingProcessor{t, notifs}
	p2 := countingProcessor{t, notifs}
	tlr.Subscribe(p1)
	tlr.Subscribe(p2)
	// Set a short poll timeout for tests.
	tlr.SetPollTimeout(10 * time.Millisecond)

	producer := tp.NewProducer(t, nitrous.BINLOG_KAFKA_TOPIC)
	err = producer.LogProto(context.Background(), &rpc.NitrousOp{}, nil)
	assert.NoError(t, err)

	// Confirm current lag is 1.
	lag, err := tlr.GetLag()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, lag)
	// Offsets should be empty in db.
	offs, err := offsets.RestoreBinlogOffset(p.Store, offsetkg)
	assert.NoError(t, err)
	assert.ElementsMatch(t, kafka.TopicPartitions{}, offs)

	// Start tailing and wait for the event to be processed.
	go tlr.Tail()
	for {
		lag, err := tlr.GetLag()
		assert.NoError(t, err)
		t.Logf("Lag is: %d", lag)
		if lag == 0 {
			// Sleep a brief amount of time to allow the read from consumer too
			// actually be processed.
			time.Sleep(2 * tlr.GetPollTimeout())
			break
		} else {
			time.Sleep(time.Millisecond * 10)
		}
	}
	// Both subscribers should be notified.
	assert.EqualValues(t, 2, notifs.Load())

	// Offsets should be stored in db.
	offs, err = offsets.RestoreBinlogOffset(p.Store, offsetkg)
	assert.NoError(t, err)
	assert.ElementsMatch(t, kafka.TopicPartitions{
		{Topic: ptr.To(nitrous.BINLOG_KAFKA_TOPIC), Partition: 0, Offset: 1},
	}, offs)
}
