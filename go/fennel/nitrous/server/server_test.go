package server_test

import (
	"context"
	"testing"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server"
	"fennel/nitrous/server/tailer"
	"fennel/plane"

	"github.com/stretchr/testify/assert"
)

type TestHandler struct {
	next []value.Value
}

func (th *TestHandler) ReturnNext(vals []value.Value) {
	th.next = vals
}

func (th *TestHandler) Get(ctx context.Context, duration uint32, keys []string) ([]value.Value, error) {
	return th.next, nil
}

func TestGet(t *testing.T) {
	svr := server.NewServer(nil)
	handler := &TestHandler{}
	tierId := ftypes.RealmID(1)
	aggId := ftypes.AggId(1)
	codec := rpc.AggCodec_V1
	err := svr.RegisterHandler(tierId, aggId, codec, handler)
	assert.NoError(t, err)
	expected := []value.Value{value.Int(29), value.Int(-10)}
	handler.ReturnNext(expected)
	// Getting the aggregate value for a tier without a handler.
	_, err = svr.GetAggregateValues(context.Background(), &rpc.AggregateValuesRequest{
		TierId:    2,
		AggId:     uint32(aggId),
		Codec:     codec,
		Duration:  24 * 3600,
		Groupkeys: []string{"mygk"},
	})
	assert.Error(t, err)
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

func TestRegisterDuplicate(t *testing.T) {
	svr := server.NewServer(nil)
	tierId := ftypes.RealmID(1)
	aggId := ftypes.AggId(1)
	codec := rpc.AggCodec_V1
	err := svr.RegisterHandler(tierId, aggId, codec, nil)
	assert.NoError(t, err)
	err = svr.RegisterHandler(tierId, aggId, codec, nil)
	assert.ErrorIs(t, err, server.EEXISTS)
}

func TestGetLag(t *testing.T) {
	p := plane.NewTestPlane(t)
	topic := "test-topic"
	tailer := tailer.NewTestTailer(p.Plane, topic)
	svr := server.NewServer(tailer)
	ctx := context.Background()

	// Initial lag should be 0.
	resp, err := svr.GetLag(ctx, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, resp.Lag)

	// Produce a message for tailer.
	producer := p.NewProducer(t, topic)
	err = producer.Log(ctx, []byte("hello world"), nil)
	assert.NoError(t, err)
	err = producer.Flush(time.Second)
	assert.NoError(t, err)

	// Lag should now be 1.
	resp, err = svr.GetLag(ctx, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, resp.Lag)
}
