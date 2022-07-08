//go:build !integration

package server

import (
	"fmt"
	"testing"

	"fennel/test"

	"github.com/stretchr/testify/require"
)

func TestReadRecent(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	s := NewInspector(tier, InspectorArgs{NumRecent: 10})

	messages := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		messages[i] = []byte(fmt.Sprintf("message %d", i))
	}
	s.processMessages(messages)

	logged := s.getRecentMessages()
	require.Equal(t, s.r.Len(), len(logged))
	for i, l := range logged {
		require.Equal(t, messages[99-i], l.msg, fmt.Sprintf("%v != %v", string(messages[i]), string(l.msg)))
	}
	// Log a new message
	newMsg := []byte("I'm a new message")
	s.processMessages([][]byte{newMsg})
	logged = s.getRecentMessages()
	require.Equal(t, s.r.Len(), len(logged))
	require.Equal(t, newMsg, logged[0].msg, fmt.Sprintf("%v != %v", string(newMsg), string(logged[0].msg)))
}
