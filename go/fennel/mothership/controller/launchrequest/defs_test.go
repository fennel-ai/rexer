package launchrequest

import (
	"log"
	"testing"

	"fennel/mothership"
	"fennel/mothership/model/launchrequest"
	"github.com/stretchr/testify/assert"
)

func TestProcessCompletedRequests(t *testing.T) {
	log.Print("w")
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer mothership.Teardown(m)
	log.Print("x")
	reqID, err := launchrequest.InsertRequest(m, []byte(`{}`), []byte(`{"state":"COMPLETED"}`))
	assert.NoError(t, err)
	_, err = launchrequest.InsertRequest(m, []byte(`{}`), []byte(`{"state":"PENDING"}`))
	assert.NoError(t, err)
	log.Print("y")
	completed, err := launchrequest.GetCompletedRequestIDs(m)
	assert.Len(t, completed, 1)
	assert.Equal(t, reqID, completed[0])
	log.Print("z")
	// now process the completed request and check there are no completed requests in table
	err = ProcessCompletedRequests(m)
	assert.NoError(t, err)
	completed, err = launchrequest.GetCompletedRequestIDs(m)
	assert.NoError(t, err)
	assert.Len(t, completed, 0)
}
