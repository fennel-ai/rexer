package launchrequest

import (
	"testing"

	"fennel/mothership"
	"github.com/stretchr/testify/assert"
)

func TestDB(t *testing.T) {
	m, err := mothership.Create()
	assert.NoError(t, err)

	reqID, err := InsertRequest(m, []byte(`{}`), []byte(`{"state":"COMPLETED"}`))
	assert.NoError(t, err)
	_, err = InsertRequest(m, []byte(`{}`), []byte(`{"state":"PENDING"}`))
	assert.NoError(t, err)

	completed, err := GetCompletedRequestIDs(m)
	assert.NoError(t, err)
	assert.Len(t, completed, 1)
	assert.Equal(t, reqID, completed[0])

	err = DeleteRequest(m, reqID)
	assert.NoError(t, err)
}
