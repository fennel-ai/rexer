package launchrequest

import (
	"testing"

	"fennel/mothership"
	"fennel/mothership/model/launchrequest"

	"github.com/stretchr/testify/assert"
)

func TestProcessCompletedRequests(t *testing.T) {
	t.Skip("launchrequest not used right now")
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	reqID, err := launchrequest.InsertRequest(m, []byte(`{}`), []byte(`{"state":"COMPLETED"}`))
	assert.NoError(t, err)
	_, err = launchrequest.InsertRequest(m, []byte(`{}`), []byte(`{"state":"PENDING"}`))
	assert.NoError(t, err)

	completed, err := launchrequest.GetCompletedRequestIDs(m)
	assert.Len(t, completed, 1)
	assert.Equal(t, reqID, completed[0])

	// now process the completed request and check there are no completed requests in table
	err = ProcessCompletedRequests(m)
	assert.NoError(t, err)
	completed, err = launchrequest.GetCompletedRequestIDs(m)
	assert.NoError(t, err)
	assert.Len(t, completed, 0)
}
