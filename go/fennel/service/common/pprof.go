package common

import (
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/pprof"
	"time"

	"fennel/s3"
)

type PprofArgs struct {
	PprofPort uint `arg:"--pprof-port,env:PPROF_PORT" default:"6060"`
	PprofHeapAllocThresholdBytes uint64 `arg:"--pprof-heap-alloc-threshold-bytes,env:PPROF_HEAP_ALLOC_THRESHOLD_BYTES"`
	PprofBucket string `arg:"--pprof-bucket,env:PPROF_BUCKET"`
	K8sPodName string `arg:"--k8s-pod-name,env:K8S_POD_NAME"`
}

// StartPprofServer starts a server on the "standard" 6060 port for pprof endpoints unless overridden
// Ref: https://pkg.go.dev/net/http/pprof
func StartPprofServer(port uint) {
	go func() {
		log.Println(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
	}()
}

func maybeExportProfile(t time.Time, pprofArgs PprofArgs, s3Client s3.Client) error {
	// if the bytes of allocated heap objects is greater than a set threshold, capture a heap profile and write
	// to the S3 bucket configured
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	// if threshold is not set, do not profile at all
	if pprofArgs.PprofHeapAllocThresholdBytes == 0 {
		return nil
	}

	// if the heap allocated objects require/consume lesser bytes than the configured threshold, do not profile at all
	if stats.HeapAlloc < pprofArgs.PprofHeapAllocThresholdBytes {
		return nil
	}

	// if the bucket is not specified, no destination to write to, return immediately
	if len(pprofArgs.PprofBucket) == 0 {
		return nil
	}

	var path string
	if len(pprofArgs.K8sPodName) != 0 {
		path += pprofArgs.K8sPodName
	} else {
		path += "DEFAULT_POD"
	}

	// format the current time in a readable way
	path += "_"
	path += fmt.Sprintf("%d", t.Unix())

	// convert the bytes to G and specify it in the path
	path += "_"
	path += fmt.Sprintf("%dG", stats.HeapAlloc << 30)

	// create a reader and writer using in process i/o pipe
	reader, writer := io.Pipe()

	go func() {
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(writer); err != nil {
			log.Printf("failed to write heapprofile to writer, err: %v\n", err)
		}
		writer.Close()
	}()

	if err := s3Client.Upload(reader, path, pprofArgs.PprofBucket); err != nil {
		return fmt.Errorf("failed to the upload the profile to S3 bucket, err: %w", err)
	}

	log.Printf("successfully uploaded profile, key: %s when the size of the heap allocated objects was: %dG", path, stats.HeapAlloc << 30)
	return nil
}

func StartProfileExporter(pprofArgs PprofArgs, s3client s3.Client) {
	// run every 2 minutes
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
			case t := <-ticker.C:
				err := maybeExportProfile(t, pprofArgs, s3client)
				if err != nil {
					log.Printf("found err: %v at time: %v\n", err, t)
				}
		}
	}
}