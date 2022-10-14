package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"fennel/featurestore/tier"
	grpcLib "fennel/lib/featurestore/grpc/proto"
	"github.com/alexflint/go-arg"
	"google.golang.org/grpc"
)

func main() {
	// seed random number generator so that all uses of rand work well
	rand.Seed(time.Now().UnixNano())
	var flags struct {
		tier.TierArgs
	}
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	tier, err := tier.CreateFromArgs(&flags.TierArgs)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup tier connectors: %v", err))
	}

	lis, err := net.Listen("tcp", "localhost:1234")
	if err != nil {
		log.Fatalf("grpc server failed to listen: %v", err)
	}

	var opts []grpc.ServerOption
	server := grpc.NewServer(opts...)

	grpcLib.RegisterFennelFeatureStoreServer(server, &featureStoreServer{
		UnimplementedFennelFeatureStoreServer: grpcLib.UnimplementedFennelFeatureStoreServer{},
		tier:                                  tier,
	})

	log.Print("listening")
	err = server.Serve(lis)
	if err != nil {
		log.Fatalf("failed")
	}
}
