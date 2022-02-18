package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	httplib "fennel/lib/http"
	_ "fennel/opdefs"
	"fennel/service/common"
	"fennel/tier"

	"github.com/alexflint/go-arg"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//------------------------ START metric definitions ----------------------------

var totalRequests = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Number of get requests.",
	},
	[]string{"path"},
)

var responseStatus = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "response_status",
		Help: "Status of HTTP response",
	},
	[]string{"path", "status"},
)

var httpDuration = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "http_response_time_seconds",
	Help: "Duration of HTTP requests.",
	// Track quantiles within small error
	Objectives: map[float64]float64{
		0.25: 0.05,
		0.50: 0.05,
		0.75: 0.05,
		0.90: 0.05,
		0.95: 0.02,
		0.99: 0.01,
	},
}, []string{"path"})

//------------------------ END metric definitions ------------------------------

// response writer to capture status code from header.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func NewResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{w, http.StatusOK}
}

// middleware to "log" response codes, latency histogram and count total number
// of requests.
func prometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := mux.CurrentRoute(r)
		path, _ := route.GetPathTemplate()
		timer := prometheus.NewTimer(httpDuration.WithLabelValues(path))
		rw := NewResponseWriter(w)
		next.ServeHTTP(rw, r)
		statusCode := rw.statusCode
		timer.ObserveDuration()
		responseStatus.WithLabelValues(path, strconv.Itoa(statusCode)).Inc()
		totalRequests.WithLabelValues(path).Inc()
	})
}

func main() {
	// Parse flags / environment variables.
	var flags struct {
		tier.TierArgs
		common.PrometheusArgs
	}
	arg.MustParse(&flags)

	router := mux.NewRouter()

	// Start a prometheus server and add a middleware to the main router to capture
	// standard metrics.
	common.StartPromMetricsServer(flags.MetricsPort)
	router.Use(prometheusMiddleware)
	router.Use(httplib.TimeoutMiddleware(2 * time.Second))
	router.Use(httplib.RateLimitingMiddleware(1000))

	tier, err := tier.CreateFromArgs(&flags.TierArgs)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup tier connectors: %v", err))

	}
	controller := server{tier}
	controller.setHandlers(router)

	addr := fmt.Sprintf(":%d", httplib.PORT)
	log.Printf("starting http service on %s...", addr)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Listen(): %v", err)
		panic(err)
	}

	// Signal that server is open for business.
	// Note: don't delete this log line - e2e tests rely on this to be printed
	// to know that server has initialized and is ready to take traffic
	log.Println("server is ready...")

	if err = http.Serve(l, router); err != http.ErrServerClosed {
		log.Fatalf("Serve(): %v", err)
		panic(err)
	}
}
