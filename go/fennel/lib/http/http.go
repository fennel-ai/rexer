package http

import (
	"math/rand"
	"net/http"
	"time"

	"fennel/lib/tracer"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	PORT = 2425
)

// TODO: write a test
func TimeoutMiddleware(timeout time.Duration) mux.MiddlewareFunc {
	return func(h http.Handler) http.Handler {
		return http.TimeoutHandler(h, timeout, "server timed out")
	}
}

// TODO: write a test
func RateLimitingMiddleware(maxConcurrentRequests int) mux.MiddlewareFunc {
	bucket := make(chan struct{}, maxConcurrentRequests)
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case bucket <- struct{}{}:
				h.ServeHTTP(w, r)
				<-bucket
			case <-r.Cancel:
				return
			}
		})
	}
}

// Tracer returns a middleware which starts tracing each http request. When request is finished,
// it logs the tracing data if the request took more than `slowThreshold` time. If not, it logs
// the trace of a random fraction of all requests
func Tracer(log *zap.Logger, slowThreshold time.Duration, sampleRate float64) mux.MiddlewareFunc {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			route := mux.CurrentRoute(r)
			path, _ := route.GetPathTemplate()
			start := time.Now()
			span := tracer.StartSpan(r.Context(), path)
			traceId := span.GetXrayTraceID()
			rw.Header().Add("rexer-traceid", traceId)
			h.ServeHTTP(rw, r.WithContext(span.Context()))
			span.End()
			if time.Since(start) > slowThreshold || rand.Float64() < sampleRate {
				log.Info("trace", zap.String("xray-id", traceId))
			}
		})
	}
}
