package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"fennel/lib/timer"
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
			case <-r.Context().Done():
				return
			}
		})
	}
}

// Tracer returns a middleware which starts tracing each http request. When request is finished,
// it logs the tracing data if the request took more than `slowThreshold` time. If not, it logs
// the trace of a random fraction of all requests
func Tracer(log *zap.Logger, slowThreshold time.Duration) mux.MiddlewareFunc {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			route := mux.CurrentRoute(r)
			path, _ := route.GetPathTemplate()
			start := time.Now()
			c := context.WithValue(r.Context(), timer.TraceKey{}, timer.TraceVal{})
			ctx, span := otel.Tracer("fennel").Start(c, path)
			traceId := timer.GetXrayTraceID(span)
			rw.Header().Add("rexer-traceid", traceId)
			h.ServeHTTP(rw, r.WithContext(ctx))
			span.End()
			dur := time.Since(start)
			if dur > slowThreshold && span.SpanContext().IsSampled() {
				log.Info(fmt.Sprintf("x-ray traceid: %s, took: %dms", traceId, dur.Milliseconds()))
			}
		})
	}
}
