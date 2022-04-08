package common

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
)

type PprofArgs struct {
	PprofPort uint `arg:"--pprof-port,env:PPROF_PORT" default:"6060"`
}

// StartPprofServer starts a server on the "standard" 6060 port for pprof endpoints unless overriden
// Ref: https://pkg.go.dev/net/http/pprof
func StartPprofServer(port uint) {
	go func() {
		log.Println(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
	}()
}
