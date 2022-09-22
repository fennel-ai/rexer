package main

import (
	"context"
	_ "expvar"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"fennel/controller/usage"
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

func installPythonPackages() error {
	cmd := exec.Command("pip3", "install", "--extra-index-url=https://token:e117e0d0267d75d4bd73bb8ca0c5b5819b9a549f@api.packagr.app/mVIM1fJ", "rexerclient")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
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
		common.HealthCheckArgs
	}
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	tier, err := tier.CreateFromArgs(&flags.TierArgs)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup tier connectors: %v", err))
	}

	router := mux.NewRouter()

	// Start a prometheus server and add a middleware to the main router to capture
	// standard metrics.
	common.StartPromMetricsServer(flags.MetricsPort)
	// Start health checker to export readiness and liveness state for the container running the server
	common.StartHealthCheckServer(flags.HealthPort)
	// Start a pprof server to export the standard pprof endpoints.
	profiler := common.CreateProfiler(flags.PprofArgs)
	profiler.StartPprofServer()

	router.Use(prometheusMiddleware)
	// TODO: add-back timeout and rate-limiting middleware once system is more
	// consistently functioning end-to-end.
	// router.Use(httplib.TimeoutMiddleware(2 * time.Second))
	// router.Use(httplib.RateLimitingMiddleware(1000))
	router.Use(httplib.Tracer(tier.Logger, time.Millisecond*500))
	cctx, controllerCancel := context.WithCancel(context.Background())
	defer controllerCancel()
	usageController := usage.NewController(cctx, &tier, 10*time.Second, 50, 50, 1000)
	controller := NewServer(&tier, usageController)
	defer controller.Close()
	controller.setHandlers(router)
	// Set handlers for the log inspector.
	inspector := inspector.NewInspector(tier, flags.InspectorArgs)
	inspector.SetHandlers(router)

	addr := fmt.Sprintf(":%d", httplib.PORT)
	log.Printf("starting http service on %s...", addr)

	err = installPythonPackages()
	if err != nil {
		panic(err)
	}

	// Run memory watchdog.
	memory.RunMemoryWatchdog(time.Minute)
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, syscall.SIGTERM, syscall.SIGINT)

	srv := &http.Server{
		Addr:    addr,
		Handler: router,
	}

	go func() {
		// `ListenAndServer` listens on the TCP network address at `srv.Addr` and then calls Server to handle
		// requests on incoming connections
		if err = srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Serve(): %v", err)
		}
	}()

	// start profile writer
	go func() {
		profiler.StartProfileExporter(tier.S3Client)
	}()

	// Signal that server is open for business.
	// Note: don't delete this log line - e2e tests rely on this to be printed
	// to know that server has initialized and is ready to take traffic
	log.Println("server is ready...")

	<-stopped
	// Close Stop server so that goroutines can cleanly exit.
	log.Println("server stopped...")

	// Shutdown gracefully shuts down the server without interrupting any active connections.
	//
	// Shutdown waits indefinitely for connections to return to idle and then shut down - therefore we pass a context
	// with 30 seconds timeout - so that this method does not wait indefinitely but waits enough time for the
	// ongoing requests or connections to finish
	//
	// NOTE: Shutdown does not attempt to close nor wait for "hijacked" connections such as WebSockets - these needs to
	// separately notify those connections to shutdown and wait for them to close.
	// `RegisterOnShutdown` is a way to register those notification functions
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("server shutdown failed: %v", err)
	}
	log.Println("server exited properly...")
}
