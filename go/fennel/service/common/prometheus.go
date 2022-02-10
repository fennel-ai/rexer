package common

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type PrometheusArgs struct {
	MetricsPort uint `arg:"--metrics-port,env:METRICS_PORT" default:"2112"`
}

func StartPromMetricsServer(port uint) {
	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", port), router)
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("metric server stopped unexpectedly: %v", err)
		}
	}()
}
