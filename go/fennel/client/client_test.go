package client

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/stretchr/testify/assert"
)

func TestClient_GetAggregateValue(t *testing.T) {
	// to test - if we call client with correct args, it calls server
	// with the correct serialized stuff and deserializes the response back
	expected := value.Int(1)
	ser := value.ToJSON(expected)
	aggname := ftypes.AggName("somename")
	k := value.Bool(true)
	kwargs := value.NewDict(map[string]value.Value{"duration": value.Int(120)})
	agvr := aggregate.GetAggValueRequest{
		AggName: aggname,
		Key:     k,
		Kwargs:  kwargs,
	}
	expReq, err := json.Marshal(agvr)
	assert.NoError(t, err)

	// now setup the server
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// server should verify that the request body is simply the serialized json
		req, err := ioutil.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.Equal(t, expReq, req)
		w.Write(ser)
	}))
	defer svr.Close()
	c, err := NewClient(svr.URL, svr.Client())
	assert.NoError(t, err)

	found, err := c.GetAggregateValue(aggname, k, kwargs)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}
