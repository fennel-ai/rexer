package main

import (
	"context"
	"fennel/s3"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	exec "golang.org/x/sys/execabs"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"
	"time"
)


const EXT_REST_VERSION = "/v1"
const INT_REST_VERSION = "/internal/v1"

type TrafficCaptureArgs struct {
	GorDir string `arg:"--gor-dir,env:GOR_DIR" json:"gor_dir,omitempty"`
	BucketName string `arg:"--bucketname,env:BUCKET_NAME" json:"bucket_name,omitempty"`
	Region string `arg:"--region,env:REGION" json:"region,omitempty"`
	Port uint32 `arg:"--port,env:PORT" json:"port,omitempty"`
}

func validateArgs(args TrafficCaptureArgs) {
	if args.GorDir == "" {
		panic(fmt.Errorf("--gor-dir cannot be empty"))
	}

	if args.BucketName == "" {
		panic(fmt.Errorf("--bucketname cannot be empty"))
	}

	if args.Region == "" {
		panic(fmt.Errorf("--region cannot be empty"))
	}

	if args.Port == 0 {
		panic(fmt.Errorf("--port cannot be unset"))
	}
}

func StartGor(servAddr, path string) error {
	// log requests at the hour level

	// Writes the files every hour (unless the local buffer is not full) - it can write multiple files as well
	// in case there too many requests
	//
	// TCP dump parameters to make this work for our query calls which contain a huge AST serialized as JSON
	cmd := exec.Command("./gor", "--input-raw", servAddr, "--output-file", filepath.Join(path, "requests-%Y-%m-%d-%H.log"),
		"--input-raw-buffer-size", "4MB", "--output-http-response-buffer", "4MB", "--input-raw-override-snaplen")
	stdErr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	err = cmd.Start()
	if err != nil {
		zap.L().Warn("gor failed with", zap.Error(err))
		return err
	}
	zap.L().Info("gor started successfully, waiting for them to finish")
	slurp, _ := io.ReadAll(stdErr)
	zap.L().Info("", zap.String("stderr", string(slurp)))
	err = cmd.Wait()
	if err != nil {
		zap.L().Warn("gor finished with error", zap.Error(err))
		return err
	}
	zap.L().Info("gor finished successfully, exiting")
	return nil
}

func uploadFilesToBucket(s3Client s3.Client, gorDir, bucketName string) error {
	files, err := ioutil.ReadDir(gorDir)
	if err != nil {
		return fmt.Errorf("failed to list files in the dir: %s, %v", gorDir, err)
	}
	t := time.Now()
	for _, file := range files {
		fname := file.Name()
		f, err := os.Open(filepath.Join(gorDir, fname))
		if err != nil {
			return fmt.Errorf("failed to read file: %s, err: %v", fname, err)
		}
		key := path.Join(t.Format("2006/01/02/03"), fname)
		if err := s3Client.Upload(f, key, bucketName); err != nil {
			return fmt.Errorf("failed to upload file: %s, err: %v", fname, err)
		}

		// once uploaded, delete the file
		if err := os.Remove(filepath.Join(gorDir, fname)); err != nil {
			return fmt.Errorf("failed to delete local file: %s, %v", fname, err)
		}
	}
	return nil
}

func Query(w http.ResponseWriter, req *http.Request) {
	// do nothing here
	w.WriteHeader(http.StatusOK)
}

func main() {
	args := TrafficCaptureArgs{}
	arg.MustParse(&args)
	validateArgs(args)

	fmt.Print("Creating logger")
	config := zap.NewProductionConfig()
	config.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	logger, err := config.Build(
		zap.AddCaller(),
		zap.AddStacktrace(zap.ErrorLevel),
	)
	if err != nil {
		panic(fmt.Errorf("failed to construct logger: %v", err))
	}
	_ = zap.ReplaceGlobals(logger)

	if err := os.MkdirAll(args.GorDir, os.ModePerm); err != nil {
		panic(fmt.Errorf("could not create directory: %v", err))
	}

	servAddr := fmt.Sprintf(":%d", args.Port)

	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, syscall.SIGTERM, syscall.SIGINT)

	// create s3 client
	s3Args := s3.S3Args{
		Region: args.Region,
	}
	s3Client := s3.NewClient(s3Args)

	// go routine which will scan and write the files remotely to S3 bucket every hour?
	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		for  {
			select {
			case <-stopped:
				zap.L().Info("server was stopped, uploading all files to s3")
				if err := uploadFilesToBucket(s3Client, args.GorDir, args.BucketName); err != nil {
					zap.L().Error("failed to upload to s3 bucket", zap.Error(err))
				}
				return
			case <-ticker.C:
				// periodically remote write the request files
				if err := uploadFilesToBucket(s3Client, args.GorDir, args.BucketName); err != nil {
					zap.L().Error("failed to upload to s3 bucket", zap.Error(err))
				}
			}
		}
	}()

	router := mux.NewRouter()
	router.HandleFunc("/query", Query)
	router.HandleFunc(INT_REST_VERSION + "/query", Query)
	router.HandleFunc(EXT_REST_VERSION + "/query", Query)
	srv := &http.Server{
		Addr: servAddr,
		Handler: router,
	}

	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Fatal("Listen and server failed", zap.Error(err))
		}
	}()

	logger.Info("server started..")

	go func() {
		for {
			if err := StartGor(servAddr, args.GorDir); err != nil {
				zap.L().Error("failed to start gor", zap.Error(err))
			}
		}
	}()

	<-stopped
	logger.Info("server stopped..")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("server shutdown failed: %v", err)
	}
	log.Println("server exited properly...")
}