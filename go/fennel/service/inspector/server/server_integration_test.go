//go:build integration

package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"fennel/lib/feature"
	"fennel/test"

	"github.com/stretchr/testify/require"
)

func TestReadRecentIntegration(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	s := NewInspector(tier, InspectorArgs{NumRecent: 10})
	err = s.startFeatureLogTailer()
	require.NoError(t, err)

	p := tier.Producers[feature.KAFKA_TOPIC_NAME]
	messages := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		messages[i] = []byte(fmt.Sprintf("message %d", i))
		err = p.Log(ctx, messages[i], nil)
		require.NoError(t, err)
	}
	err = p.Flush(time.Second)
	require.NoError(t, err)

	// Wait for messages to be consumed.
	time.Sleep(30 * time.Second)
	logged := s.getRecentMessages()
	require.Equal(t, s.r.Len(), len(logged))
	for i, l := range logged {
		require.Equal(t, messages[99-i], l.msg, fmt.Sprintf("%v != %v", string(messages[i]), string(l.msg)))
	}

	// Log a new message
	newMsg := []byte("I'm a new message")
	err = p.Log(ctx, newMsg, nil)
	require.NoError(t, err)
	err = p.Flush(time.Second)
	// Wait for new message to also be consumed.
	time.Sleep(30 * time.Second)
	logged = s.getRecentMessages()
	require.Equal(t, s.r.Len(), len(logged))
	require.Equal(t, newMsg, logged[0].msg, fmt.Sprintf("%v != %v", string(newMsg), string(logged[0].msg)))
}
