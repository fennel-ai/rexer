package main

import (
	"testing"
	"time"

	"fennel/mothership"
	"fennel/mothership/model/launchrequest"
	"github.com/alexflint/go-arg"
	"github.com/stretchr/testify/assert"
)

func TestPollLaunchRequests(t *testing.T) {
	var flags struct {
		mothership.MothershipArgs
		BridgeArgs
	}
	arg.Parse(&flags)
	m, err := mothership.CreateFromArgs(&flags.MothershipArgs)
	assert.NoError(t, err)

	go pollLaunchRequests(m)
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
