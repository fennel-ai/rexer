package launchrequest

/*
func TestProcessCompletedRequests(t *testing.T) {
	m, err := mothership.CreateTestMothership()
	assert.NoError(t, err)
	defer mothership.Teardown(m)

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
*/
