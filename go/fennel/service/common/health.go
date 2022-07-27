package common

import (
	"fmt"
	"github.com/heptiolabs/healthcheck"
	"log"
	"net/http"
)

type HealthCheckArgs struct {
	HealthPort uint `arg:"--health-port,env:HEALTH_PORT" default:"8082"`
}

func StartHealthCheckServer(port uint) {
	health := healthcheck.NewHandler()

	// TODO(mohit): use health.AddReadinessCheck() to check readiness of downstream services (DB, redis, sagemaker etc)
	// so that the server is marked as ready only if the downstream servers are functioning.
	//
	// The readiness check helps with safe rollouts and serving traffic with "ready" machines
	// i) Deployment stops updating the pods if any of the container is not ready
	// ii) Kubernetes endpoint corresponding to the pod is removed from healthy backends if it is marked not ready

	go func() {
		err := http.ListenAndServe(fmt.Sprintf(":%d", port), health)
		if err != nil {
			log.Fatalf("health check server stopped unexpectedly: %v", err)
		}
	}()
}