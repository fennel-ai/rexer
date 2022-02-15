package http

import (
	"context"
	"net/http"
	"time"
)

const (
	PORT = 2425
)

// TODO: write a test

func TimeoutMiddleware(timeout time.Duration) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()

			r = r.WithContext(ctx)

			done := make(chan bool)
			go func() {
				h.ServeHTTP(w, r)
				done <- true
			}()

			select {
			case <-ctx.Done():
				http.Error(w, "server timeout", http.StatusGatewayTimeout)
			case <-done:
			}
		})
	}
}

// TODO: write a test
func RateLimitingMiddleware(maxConnections int) func(h http.Handler) http.Handler {
	ratelimit := make(chan struct{}, maxConnections)
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ratelimit <- struct{}{}
			defer func() { <-ratelimit }()
			h.ServeHTTP(w, r)
		})
	}
}
