package tailer_test

import (
	"context"
	"testing"
	"time"

	"fennel/hangar"
	"fennel/lib/nitrous"
	"fennel/lib/utils/ptr"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server/offsets"
	"fennel/nitrous/server/tailer"
	"fennel/nitrous/test"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type countingProcessor struct {
	t       *testing.T
	counter *atomic.Int32
}

func (c countingProcessor) Identity() string {
	return "countingProcessor"
}

func (c countingProcessor) Process(ctx context.Context, ops []*rpc.NitrousOp, store hangar.Reader) ([]hangar.Key, []hangar.ValGroup, error) {
	c.t.Log("got notified")
	_ = c.counter.Inc()
	return nil, nil, nil
}

var _ tailer.EventProcessor = countingProcessor{}

var offsetkey = []byte("offsetkey")

func TestTailer(t *testing.T) {
	n := test.NewTestNitrous(t)
	ctx := context.Background()

	// Create the producer first so the topic is initialized.
	producer := n.NewBinlogProducer(t)

	tlr, err := tailer.NewTailer(n.Nitrous, nitrous.BINLOG_KAFKA_TOPIC, nil, offsetkey)
	assert.NoError(t, err)
	notifs := atomic.NewInt32(0)
	p1 := countingProcessor{t, notifs}
	p2 := countingProcessor{t, notifs}
	tlr.Subscribe(p1)
	tlr.Subscribe(p2)

	err = producer.LogProto(context.Background(), &rpc.NitrousOp{}, nil)
	assert.NoError(t, err)

	// Set a shorter poll timeout for tests.
	tlr.SetPollTimeout(5 * time.Second)
	// Start tailing and wait for the consumer to be assigned partitions.
	// Before the consumer is assigned partitions, it is not possible to measure
	// the lag.
	go tlr.Tail()
	for {
		offs, err := tlr.GetOffsets()
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
	offvgs, err := n.Store.GetMany(ctx, []hangar.KeyGroup{{Prefix: hangar.Key{Data: offsetkey}}})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(offvgs))
	toppars, err := offsets.DecodeOffsets(offvgs[0])
	assert.NoError(t, err)
	assert.ElementsMatch(t, kafka.TopicPartitions{}, toppars)

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
	// Both subscribers should be notified.
	assert.EqualValues(t, 2, notifs.Load())

	// Offsets should be stored in db.
	scope := resource.NewPlaneScope(n.PlaneID)
	offvgs, err = n.Store.GetMany(ctx, []hangar.KeyGroup{{Prefix: hangar.Key{Data: offsetkey}}})
	assert.NoError(t, err)
	require.Equal(t, 1, len(offvgs))
	toppars, err = offsets.DecodeOffsets(offvgs[0])
	assert.NoError(t, err)
	assert.ElementsMatch(t, kafka.TopicPartitions{
		{Topic: ptr.To(scope.PrefixedName(nitrous.BINLOG_KAFKA_TOPIC)), Partition: 0, Offset: 1},
	}, toppars)
}
