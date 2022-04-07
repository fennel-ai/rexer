package common

import (
	"log"
	"net/http"
	_ "net/http/pprof"
)

// Start a server on the "standard" 6060 port for pprof endpoints.
// Ref: https://pkg.go.dev/net/http/pprof
func StartPprofServer() {
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
}
