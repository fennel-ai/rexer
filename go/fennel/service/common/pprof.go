package common

import (
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"path"
	"runtime"
	"runtime/pprof"
	"time"

	"fennel/s3"
)

type PprofArgs struct {
	PprofPort uint `arg:"--pprof-port,env:PPROF_PORT" default:"6060"`
	PprofHeapAllocThresholdBytes uint64 `arg:"--pprof-heap-alloc-threshold-bytes,env:PPROF_HEAP_ALLOC_THRESHOLD_BYTES"`
	PprofBucket string `arg:"--pprof-bucket,env:PPROF_BUCKET"`
	ProcessId string `arg:"--process-id,env:PROCESS_ID" default:"DEFAULT"`
}

type Profiler struct {
	args PprofArgs
}

func CreateProfiler(args PprofArgs) Profiler {
	return Profiler{args: args}
}

// StartPprofServer starts a server on the "standard" 6060 port for pprof endpoints unless overridden
// Ref: https://pkg.go.dev/net/http/pprof
func (p Profiler) StartPprofServer() {
	go func() {
		log.Println(http.ListenAndServe(fmt.Sprintf(":%d", p.args.PprofPort), nil))
	}()
}

func (p Profiler) StartProfileExporter(s3client s3.Client) {
	// run every 2 minutes
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case t := <-ticker.C:
			err := maybeExportProfile(t, p.args, s3client)
			if err != nil {
				log.Printf("found err: %v at time: %v\n", err, t)
			}
		}
	}
}

func maybeExportProfile(t time.Time, pprofArgs PprofArgs, s3Client s3.Client) error {
	// TODO(mohit): Add sampling here so that we don't keep exporting the profiles in case the process is under load

	// if threshold is not set, do not profile at all
	if pprofArgs.PprofHeapAllocThresholdBytes == 0 {
		return nil
	}

	// if the bucket is not specified, no destination to write to, return immediately
	if len(pprofArgs.PprofBucket) == 0 {
		return nil
	}

	// if the bytes of allocated heap objects is greater than a set threshold, capture a heap profile and write
	// to the S3 bucket configured
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	// if the heap allocated objects require/consume lesser bytes than the configured threshold, do not profile at all
	if stats.HeapAlloc < pprofArgs.PprofHeapAllocThresholdBytes {
		return nil
	}

	// create an object in the bucket with the following folder structure:
	// 	yyyy-mm-dd/pod_name/timestampMillis_heapAllocSpace
	key := path.Join(t.Format("2006/01/02"), pprofArgs.ProcessId, fmt.Sprintf("%d_%dG", t.UnixMilli(), stats.HeapAlloc >> 30))

	// create a reader and writer using in process i/o pipe
	reader, writer := io.Pipe()

	go func() {
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(writer); err != nil {
			log.Printf("failed to write heapprofile to writer, err: %v\n", err)
		}
		writer.Close()
	}()

	if err := s3Client.Upload(reader, key, pprofArgs.PprofBucket); err != nil {
		return fmt.Errorf("failed to the upload the profile to S3 bucket, err: %w", err)
	}

	log.Printf("successfully uploaded profile, key: %s when the size of the heap allocated objects was: %dG", key, stats.HeapAlloc >> 30)
	return nil
}