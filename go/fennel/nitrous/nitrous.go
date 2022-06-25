package nitrous

import (
	"fmt"
	"net"

	"fennel/lib/nitrous"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server"
	"fennel/nitrous/server/metadata"
	"fennel/nitrous/server/offsets"
	"fennel/nitrous/server/tailer"
	"fennel/plane"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// StartServer starts a Nitrous server instance. This function blocks until
// termination.
func StartServer(plane plane.Plane, listener net.Listener) error {
	// Initialize binlog tailer.
	offsetkg := []byte("default_tailer")
	offsets, err := offsets.RestoreBinlogOffset(plane.Store, offsetkg)
	if err != nil {
		plane.Logger.Fatal("Failed to restore binlog offsets from hangar", zap.Error(err))
	}
	tailer, err := tailer.NewTailer(plane, nitrous.BINLOG_KAFKA_TOPIC, offsets, offsetkg)
	if err != nil {
		return fmt.Errorf("failed to setup tailer: %w", err)
	}

	// Setup server.
	svr := server.NewServer(tailer)

	// Restore aggregate definitions.
	adm := metadata.NewAggDefsMgr(plane, tailer, svr)
	if err != nil {
		return fmt.Errorf("failed to setup aggregate definitions manager: %w", err)
	}
	err = adm.RestoreAggregates()
	if err != nil {
		return fmt.Errorf("failed to restore aggregate definitions: %w", err)
	}

	// Start tailing the binlog. We do this after restoring the aggregates, so
	// that they don't miss any events.
	go tailer.Tail()

	// Setup the grpc server.
	grpcServer := grpc.NewServer()
	rpc.RegisterNitrousServer(grpcServer, svr)

	// Finally, start the server.
	if err = grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}
