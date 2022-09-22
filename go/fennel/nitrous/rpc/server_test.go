package rpc_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/nitrous/rpc"

	"github.com/stretchr/testify/assert"
)

type TestDB struct {
	next []value.Value
	lag  int
}

func (tdb *TestDB) ReturnNext(vals []value.Value) {
	tdb.next = vals
}

func (tdb *TestDB) setLag(lag int) {
	tdb.lag = lag
}

func (tdb *TestDB) Get(ctx context.Context, tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec, groupkeys []string, kwargs []value.Dict) ([]value.Value, error) {
	if tdb.next == nil {
		return nil, fmt.Errorf("no values")
	}
	return tdb.next, nil
}

func (tdb *TestDB) GetLag() (int, error) {
	return tdb.lag, nil
}

func (tdb *TestDB) Stop() {}

func (tdb *TestDB) SetBinlogPollTimeout(time.Duration) {}

func (tdb *TestDB) GetBinlogPollTimeout() time.Duration {
	return 0
}

// func TestGet(t *testing.T) {
// 	testdb := &TestDB{}
// 	svr := rpc.NewServer(testdb)
// 	tierId := ftypes.RealmID(1)
// 	aggId := ftypes.AggId(1)
// 	codec := rpc.AggCodec_V2
// 	kwargs := value.NewDict(nil)
// 	kwargs.Set("duration", value.Int(24*3600))
// 	pkwargs, err := value.ToProtoDict(kwargs)
// 	assert.NoError(t, err)
// 	_, err = svr.GetAggregateValues(context.Background(), &rpc.AggregateValuesRequest{
// 		TierId:    2,
// 		AggId:     uint32(aggId),
// 		Codec:     codec,
// 		Kwargs:    []*value.PVDict{&pkwargs},
// 		Groupkeys: []string{"mygk"},
// 	})
// 	assert.Error(t, err)
// 	expected := []value.Value{value.Int(29), value.Int(-10)}
// 	testdb.ReturnNext(expected)
// 	resp, err := svr.GetAggregateValues(context.Background(), &rpc.AggregateValuesRequest{
// 		TierId:    uint32(tierId),
// 		AggId:     uint32(aggId),
// 		Codec:     codec,
// 		Kwargs:    []*value.PVDict{&pkwargs},
// 		Groupkeys: []string{"mygk1", "mygk2"},
// 	})
// 	assert.NoError(t, err)
// 	assert.Equal(t, len(expected), len(resp.Results))
// 	for i, e := range expected {
// 		got, err := value.FromProtoValue(resp.Results[i])
// 		assert.NoError(t, err)
// 		assert.Equal(t, e, got)
// 	}
// }

func TestGetLag(t *testing.T) {
	testdb := &TestDB{}
	svr := rpc.NewServer(testdb)
	ctx := context.Background()

	// Initial lag should be 0.
	resp, err := svr.GetLag(ctx, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, resp.Lag)

	testdb.setLag(1)
	resp, err = svr.GetLag(ctx, nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, resp.Lag)
}
