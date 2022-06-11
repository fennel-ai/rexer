package main

import (
	_ "expvar"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"time"

	httplib "fennel/lib/http"
	"fennel/lib/timer"
	"fennel/lib/utils/memory"
	_ "fennel/opdefs"
	"fennel/service/common"
	inspector "fennel/service/inspector/server"
	"fennel/tier"

	"github.com/alexflint/go-arg"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ------------------------ START metric definitions ----------------------------

var totalRequests = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Number of incoming HTTP requests.",
	},
	[]string{"path"},
)

var totalRequestsProcessed = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "http_requests_processed_total",
		Help: "Number of HTTP requests processed.",
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

// ------------------------ END metric definitions ------------------------------

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
		totalRequests.WithLabelValues(path).Inc()
		timer := prometheus.NewTimer(httpDuration.WithLabelValues(path))
		rw := NewResponseWriter(w)
		next.ServeHTTP(rw, r)
		statusCode := rw.statusCode
		timer.ObserveDuration()
		responseStatus.WithLabelValues(path, strconv.Itoa(statusCode)).Inc()
		totalRequestsProcessed.WithLabelValues(path).Inc()
	})
}

func main() {
	// seed random number generator so that all uses of rand work well
	rand.Seed(time.Now().UnixNano())
	// Parse flags / environment variables.
	var flags struct {
		tier.TierArgs
		common.PrometheusArgs
		common.PprofArgs
		inspector.InspectorArgs
		timer.TracerArgs
	}
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.SetOutput(ioutil.Discard)
	tier, err := tier.CreateFromArgs(&flags.TierArgs)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup tier connectors: %v", err))
	}
	fmt.Printf("Tier: %v\n", tier)
	router := mux.NewRouter()

	// Start a prometheus server and add a middleware to the main router to capture
	// standard metrics.
	common.StartPromMetricsServer(flags.MetricsPort)
	// Start a pprof server to export the standard pprof endpoints.
	common.StartPprofServer(flags.PprofPort)

	router.Use(prometheusMiddleware)
	// TODO: add-back timeout and rate-limiting middleware once system is more
	// consistently functioning end-to-end.
	// router.Use(httplib.TimeoutMiddleware(2 * time.Second))
	// router.Use(httplib.RateLimitingMiddleware(1000))
	router.Use(httplib.Tracer(tier.Logger, time.Millisecond*500, 0))

	controller := server{tier}
	controller.setHandlers(router)
	// Set handlers for the log inspector.
	inspector := inspector.NewInspector(tier, flags.InspectorArgs)
	inspector.SetHandlers(router)

	addr := fmt.Sprintf(":%d", httplib.PORT)
	log.Printf("starting http service on %s...", addr)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Listen(): %v", err)
	}

	// Run memory watchdog.
	memory.RunMemoryWatchdog(time.Minute)

	// Signal that server is open for business.
	// Note: don't delete this log line - e2e tests rely on this to be printed
	// to know that server has initialized and is ready to take traffic
	log.Println("server is ready...")

	if err = http.Serve(l, router); err != http.ErrServerClosed {
		log.Fatalf("Serve(): %v", err)
	}
}
