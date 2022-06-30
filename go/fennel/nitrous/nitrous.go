package nitrous

import (
	"fmt"
	"net"

	"fennel/hangar"
	"fennel/lib/nitrous"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server"
	"fennel/nitrous/server/metadata"
	"fennel/nitrous/server/offsets"
	"fennel/nitrous/server/tailer"
	"fennel/plane"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// StartServer starts a Nitrous server instance. This function blocks until
// termination.
func StartServer(plane plane.Plane, listener net.Listener) error {
	// Initialize binlog tailer.
	offsetkey := []byte("default_tailer")
	vgs, err := plane.Store.GetMany([]hangar.KeyGroup{{Prefix: hangar.Key{Data: offsetkey}}})
	if err != nil {
		return fmt.Errorf("failed to get binlog offsets: %w", err)
	}
	var toppars kafka.TopicPartitions
	if len(vgs) > 0 {
		toppars, err = offsets.DecodeOffsets(vgs[0])
		if err != nil {
			plane.Logger.Fatal("Failed to restore binlog offsets from hangar", zap.Error(err))
		}
	}
	tailer, err := tailer.NewTailer(plane, nitrous.BINLOG_KAFKA_TOPIC, toppars, offsetkey)
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

	// Setup the grpc server. Add a prometheus middleware to the main router to
	// capture standard metrics.
	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
	rpc.RegisterNitrousServer(grpcServer, svr)
	// After all your registrations, make sure all of the Prometheus metrics are initialized.
	grpc_prometheus.Register(grpcServer)

	// Finally, start the server.
	if err = grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}
