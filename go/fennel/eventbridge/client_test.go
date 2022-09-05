//go:build eventbridge

package eventbridge

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEventBridgeClient(t *testing.T) {
	args := EventBridgeArgs{Region: "us-west-2"}
	client := NewClient(args)
	err := client.CreateRule("Testing", "cron(31 23 * * ? *)")
	assert.NoError(t, err)
	err = client.DeleteRule("Testing")
	assert.NoError(t, err)
}

func TestSageMakePipeline(t *testing.T) {
	args := EventBridgeArgs{Region: "us-west-2"}
	client := NewClient(args)
	err := client.CreateRule("Testing", "cron(31 23 * * ? *)")
	assert.NoError(t, err)
	err = client.DeleteRule("Testing")
	assert.NoError(t, err)
}
