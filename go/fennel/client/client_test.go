package client

import (
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestClient_GetAggregateValue(t *testing.T) {
	// to test - if we call client with correct args, it calls server
	// with the correct serialized stuff and deserializes the response back
	expected := value.Int(1)
	ser, err := value.Marshal(expected)
	assert.NoError(t, err)
	aggname := ftypes.AggName("somename")
	k := value.Bool(true)
	agvr := aggregate.GetAggValueRequest{
		AggName: aggname,
		Key:     k,
	}
	pagvr, err := aggregate.ToProtoGetAggValueRequest(agvr)
	assert.NoError(t, err)
	exp_req, err := proto.Marshal(&pagvr)
	assert.NoError(t, err)

	// now setup the server
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// server should verify that the request body is simplfy the serialized proto struct
		req, err := ioutil.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.Equal(t, exp_req, req)
		w.Write(ser)
	}))
	defer svr.Close()
	c, err := NewClient(svr.URL, svr.Client())
	assert.NoError(t, err)

	found, err := c.GetAggregateValue(aggname, k)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}
