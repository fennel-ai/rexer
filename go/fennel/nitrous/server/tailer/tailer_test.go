package tailer

import (
	"context"
	"fennel/gravel"
	"fennel/hangar/encoders"
	gravelDB "fennel/hangar/gravel"
	"testing"
	"time"

	"fennel/hangar"
	"fennel/lib/nitrous"
	"fennel/lib/utils/ptr"
	"fennel/nitrous/rpc"
	"fennel/nitrous/test"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

type countingProcessor struct {
	t       *testing.T
	counter *atomic.Int32
}

func (c countingProcessor) Process(ctx context.Context, ops []*rpc.NitrousOp, store hangar.Reader) ([]hangar.Key, []hangar.ValGroup, error) {
	c.t.Log("got notified")
	_ = c.counter.Inc()
	return nil, nil, nil
}

func TestTailer(t *testing.T) {
	n := test.NewTestNitrous(t)
	// Create the producer first so the topic is initialized.
	producer := n.NewBinlogProducer(t)
	gravelOpts := gravel.DefaultOptions()
	db, err := gravelDB.NewHangar(0, t.TempDir(), &gravelOpts, encoders.Default())
	t.Cleanup(func() { _ = db.Teardown() })
	assert.NoError(t, err)

	notifs := atomic.NewInt32(0)
	p1 := countingProcessor{t, notifs}
	tlr, err := NewTailer(n.Nitrous, nitrous.BINLOG_KAFKA_TOPIC, kafka.TopicPartition{}, db, p1.Process, DefaultPollTimeout, DefaultTailerBatch)
	assert.NoError(t, err)

	err = producer.LogProto(context.Background(), &rpc.NitrousOp{}, nil)
	assert.NoError(t, err)

	// Set a shorter poll timeout for tests.
	tlr.SetPollTimeout(5 * time.Second)
	// Start tailing and wait for the consumer to be assigned partitions.
	// Before the consumer is assigned partitions, it is not possible to measure
	// the lag.
	go tlr.Tail()
	var offs kafka.TopicPartitions
	for {
		offs, err = tlr.GetOffsets()
		assert.NoError(t, err)
		if len(offs) > 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// Confirm current lag is 1.
	lag, err := tlr.GetLag()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, lag)
	// Offsets should be empty in db.
	toppars, err := decodeOffsets(offs, tlr.store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, offs, toppars)

	for {
		lag, err := tlr.GetLag()
		assert.NoError(t, err)
		t.Logf("Lag is: %d", lag)
		if lag == 0 {
			// Sleep a brief amount of time to allow the read from consumer too
			// actually be processed.
			time.Sleep(time.Second)
			break
		} else {
			time.Sleep(tlr.GetPollTimeout())
		}
	}
	assert.EqualValues(t, 1, notifs.Load())

	// Offsets should be stored in db.
	scope := resource.NewPlaneScope(n.PlaneID)
	toppars, err = decodeOffsets(offs, tlr.store)
	assert.NoError(t, err)
	assert.ElementsMatch(t, kafka.TopicPartitions{
		{Topic: ptr.To(scope.PrefixedName(nitrous.BINLOG_KAFKA_TOPIC)), Partition: 0, Offset: 1},
	}, toppars)
}
