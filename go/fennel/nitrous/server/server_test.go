package server_test

import (
	"context"
	"testing"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server"

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
	svr := server.NewServer()
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
	svr := server.NewServer()
	tierId := ftypes.RealmID(1)
	aggId := ftypes.AggId(1)
	codec := rpc.AggCodec_V1
	err := svr.RegisterHandler(tierId, aggId, codec, nil)
	assert.NoError(t, err)
	err = svr.RegisterHandler(tierId, aggId, codec, nil)
	assert.ErrorIs(t, err, server.EEXISTS)
}
