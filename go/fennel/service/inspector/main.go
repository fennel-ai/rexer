package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"

	"fennel/service/inspector/server"
	"fennel/tier"

	"github.com/alexflint/go-arg"
	"github.com/gorilla/mux"
)

var port = flag.Uint("port", 3001, "port to listen on")

func main() {
	var flags struct {
		tier.TierArgs
		server.InspectorArgs
	}
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	tier, err := tier.CreateFromArgs(&flags.TierArgs)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup tier connectors: %v", err))
	}
	router := mux.NewRouter()
	s := server.NewInspector(tier, flags.InspectorArgs)
	s.SetHandlers(router)

	// Start server.
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("starting inspector service on %s...", addr)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Listen(): %v", err)
	}
	if err = http.Serve(l, router); err != http.ErrServerClosed {
		log.Fatalf("Serve(): %v", err)
	}
}
