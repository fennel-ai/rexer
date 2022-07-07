package server_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/nitrous"
	"fennel/lib/value"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server"
	"fennel/nitrous/server/tailer"
	"fennel/plane"

	"github.com/stretchr/testify/assert"
)

type TestDB struct {
	next []value.Value
}

func (tdb *TestDB) ReturnNext(vals []value.Value) {
	tdb.next = vals
}

func (tdb *TestDB) Get(ctx context.Context, tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec, duration uint32, groupkeys []string) ([]value.Value, error) {
	if tdb.next == nil {
		return nil, fmt.Errorf("no values")
	}
	return tdb.next, nil
}

func TestGet(t *testing.T) {
	testdb := &TestDB{}
	svr := server.NewServer(testdb, nil)
	tierId := ftypes.RealmID(1)
	aggId := ftypes.AggId(1)
	codec := rpc.AggCodec_V1
	_, err := svr.GetAggregateValues(context.Background(), &rpc.AggregateValuesRequest{
		TierId:    2,
		AggId:     uint32(aggId),
		Codec:     codec,
		Duration:  24 * 3600,
		Groupkeys: []string{"mygk"},
	})
	assert.Error(t, err)
	expected := []value.Value{value.Int(29), value.Int(-10)}
	testdb.ReturnNext(expected)
	resp, err := svr.GetAggregateValues(context.Background(), &rpc.AggregateValuesRequest{
		TierId:    uint32(tierId),
		AggId:     uint32(aggId),
		Codec:     codec,
		Duration:  24 * 3600,
		Groupkeys: []string{"mygk1", "mygk2"},
	})
	assert.NoError(t, err)
	assert.Equal(t, len(expected), len(resp.Results))
	for i, e := range expected {
		got, err := value.FromProtoValue(resp.Results[i])
		assert.NoError(t, err)
		assert.Equal(t, e, got)
	}
}

func TestGetLag(t *testing.T) {
	p := plane.NewTestPlane(t)
	// Produce a message for tailer.
	producer := p.NewBinlogProducer(t)

	tailer := tailer.NewTestTailer(p.Plane, nitrous.BINLOG_KAFKA_TOPIC)
	// Set a very long test timeout so message is not really consumed.
	tailer.SetPollTimeout(1 * time.Minute)
	go tailer.Tail()
	svr := server.NewServer(nil, tailer)
	ctx := context.Background()

	// Initial lag should be 0.
	resp, err := svr.GetLag(ctx, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, resp.Lag)

	err = producer.Log(ctx, []byte("hello world"), nil)
	assert.NoError(t, err)
	err = producer.Flush(10 * time.Second)
	assert.NoError(t, err)

	// Lag should now be 1.
	time.Sleep(5 * time.Second)
	resp, err = svr.GetLag(ctx, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, resp.Lag)
}
