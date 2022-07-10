package main

import (
	"testing"
	"time"

	"fennel/mothership"
	"fennel/mothership/model/launchrequest"
	"github.com/stretchr/testify/assert"
)

func TestPollLaunchRequestStatus(t *testing.T) {
	m, err := mothership.NewTestMothership()
	assert.NoError(t, err)
	defer func() { err = mothership.Teardown(m); assert.NoError(t, err) }()

	go pollLaunchRequestStatus(m)
	_, err = launchrequest.InsertRequest(m, []byte(`{}`), []byte(`{"state":"COMPLETED"}`))
	assert.NoError(t, err)
	_, err = launchrequest.InsertRequest(m, []byte(`{}`), []byte(`{"state":"PENDING"}`))
	assert.NoError(t, err)

	passed := false
	slept := 0
	for slept < 120 {
		completed, err := launchrequest.GetCompletedRequestIDs(m)
		assert.NoError(t, err)
		if len(completed) == 0 {
			passed = true
			break
		}
		time.Sleep(5 * time.Second)
		slept += 5
	}
	assert.True(t, passed)
}
