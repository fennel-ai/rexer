package nitrous

import (
	"context"
	"net"
	"testing"
	"time"

	"fennel/nitrous"
	"fennel/nitrous/client"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func StartNitrousServer(t *testing.T, n nitrous.Nitrous) (*rpc.Server, net.Addr) {
	// Start server.
	lis, err := net.Listen("tcp", ":0")
	assert.NoError(t, err)
	db, err := server.InitDB(n)
	assert.NoError(t, err)
	// Set a short poll timeout for tests and then start db.
	db.SetBinlogPollTimeout(1 * time.Second)
	db.SetAggrConfPollTimeout(1 * time.Second)
	db.Start()
	remote := rpc.NewServer(db)
	go func() {
		zap.L().Info("Starting nitrous server", zap.String("addr", lis.Addr().String()))
		err = remote.Serve(lis)
		assert.NoError(t, err)
	}()
	t.Cleanup(func() {
		remote.Stop()
	})
	t.Setenv("NITROUS_SERVER_ADDRESS", lis.Addr().String())
	return remote, lis.Addr()
}

func Drain(t *testing.T, c client.NitrousClient) {
	ctx := context.Background()
	count := 0
	for count < 3 {
		// Assuming that nitrous tails the log every 1s in tests.
		time.Sleep(1 * time.Second)
		lag, err := c.GetLag(ctx)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		assert.NoError(t, err)
		t.Logf("Current lag: %d", lag)
		if lag == 0 {
			count++
		}
	}
	// It is possible for the lag to be zero but the event to not have
	// been processed yet. Sleep some more to reduce the likelihood of
	// that happening.
	time.Sleep(1 * time.Second)
}

func WaitForMessagesToBeConsumed(t *testing.T, ctx context.Context, client client.NitrousClient) {
	count := 0
	for count < 3 {
		// Assuming that nitrous tails the log every 1s in tests.
		time.Sleep(1 * time.Second)
		lag, err := client.GetLag(ctx)
		assert.NoError(t, err)
		if lag == 0 {
			count++
		}
	}
}