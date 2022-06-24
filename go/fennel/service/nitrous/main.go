package main

import (
	"fmt"
	"log"
	"net"

	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server"
	"fennel/nitrous/server/metadata"
	"fennel/nitrous/server/offsets"
	"fennel/nitrous/server/tailer"
	"fennel/plane"

	"github.com/alexflint/go-arg"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var flags struct {
	ListenPort uint32 `arg:"--listen-port,env:LISTEN_PORT" json:"listen_port"`
	Binlog     string `arg:"--binlog,env:BINLOG" json:"binlog"`
	plane.PlaneArgs
}

func main() {
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	plane, err := plane.CreateFromArgs(flags.PlaneArgs)
	if err != nil {
		log.Fatalf("Failed to setup plane: %v", err)
	}

	// Initialize binlog tailer.
	offsetkg := []byte("default_tailer")
	offsets, err := offsets.RestoreBinlogOffset(plane.Store, offsetkg)
	if err != nil {
		plane.Logger.Fatal("Failed to restore binlog offsets from hangar", zap.Error(err))
	}
	tailer, err := tailer.NewTailer(plane, flags.Binlog, offsets, offsetkg)
	if err != nil {
		plane.Logger.Fatal("Failed to setup tailer", zap.Error(err))
	}

	// Setup server.
	svr := server.NewServer(tailer)

	// Restore aggregate definitions.
	adm := metadata.NewAggDefsMgr(plane, tailer, svr)
	if err != nil {
		plane.Logger.Fatal("Failed to setup aggregate definitions manager", zap.Error(err))
	}
	err = adm.RestoreAggregates()
	if err != nil {
		plane.Logger.Fatal("Failed to restore aggregate definitions", zap.Error(err))
	}

	// Start tailing the binlog. We do this after restoring the aggregates, so
	// that they don't miss any events.
	go tailer.Tail()

	// Setup the grpc server.
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", flags.ListenPort))
	if err != nil {
		plane.Logger.Fatal("Failed to listen", zap.Uint32("port", flags.ListenPort), zap.Error(err))
	}
	grpcServer := grpc.NewServer()
	rpc.RegisterNitrousServer(grpcServer, svr)

	// Finally, start the server.
	if err = grpcServer.Serve(lis); err != nil {
		plane.Logger.Fatal("gRPC server failed / could not start", zap.Error(err))
	}
}
