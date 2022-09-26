package backup

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/s3"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var backupFileSize = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "backup_s3store_filesize",
	Help: "File size for remote backup store",
	Objectives: map[float64]float64{
		0.75:  0.025,
		0.90:  0.01,
		0.95:  0.005,
		0.99:  0.001,
		0.999: 0.0001,
	},
	// Time window now is 30 seconds wide, defaults to 10m
	//
	// NOTE: we configure this > the lowest scrape interval configured for prometheus job
	MaxAge: 30 * time.Second,
	// we slide the window every 6 (= 30 / 5 ) seconds
	AgeBuckets: 5,
}, []string{"realm_id", "function_name"})

type BackupStore interface {
	Store(ctx context.Context, localFile string, remoteName string) error
	Exists(ctx context.Context, remoteName string) (bool, error)
	Fetch(ctx context.Context, remoteName string, localFile string) error
	Delete(ctx context.Context, remoteName string) error
	ListFile(ctx context.Context, remotePrefix string) ([]string, error)
}

type S3Store struct {
	region  string
	bucket  string
	prefix  string
	planeId ftypes.RealmID
	s3      *s3.Client
}

type LocalStore struct {
	backupPath string
	planeId    ftypes.RealmID
}

func NewS3Store(region string, bucketName string, dbInstanceName string, planeId ftypes.RealmID) (*S3Store, error) {
	s3Client := s3.NewClient(s3.S3Args{Region: region})
	return &S3Store{region: region, bucket: bucketName, prefix: fmt.Sprintf("backups_v1/%s", dbInstanceName), planeId: planeId, s3: &s3Client}, nil
}

func (store *S3Store) Store(ctx context.Context, localFile string, remoteName string) error {
	_, t := timer.Start(ctx, store.planeId, "backup.S3Store.Store")
	defer t.Stop()
	from, err := os.Open(localFile)
	if err != nil {
		zap.L().Error("Failed to open local file for S3 upload", zap.String("local_file", localFile), zap.Error(err))
		return err
	}
	defer from.Close()
	fi, err := from.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file statistics: %v", err)
	}
	backupFileSize.WithLabelValues(fmt.Sprintf("%d", store.planeId), "backup.S3Store.Store").Observe(float64(fi.Size()))
	fullPath := filepath.Join(store.prefix, remoteName)
	err = store.s3.Upload(from, fullPath, store.bucket)
	if err != nil {
		zap.L().Error("Failed to upload local file to s3 bucket", zap.String("s3_path", fullPath), zap.String("s3_bucket", store.bucket), zap.Error(err))
		return err
	}
	return nil
}

func (store *S3Store) Exists(ctx context.Context, remoteName string) (bool, error) {
	_, t := timer.Start(ctx, store.planeId, "backup.S3Store.Exists")
	defer t.Stop()
	fullPath := filepath.Join(store.prefix, remoteName)
	return store.s3.Exists(fullPath, store.bucket)
}

func (store *S3Store) Fetch(ctx context.Context, remoteName string, localFile string) error {
	_, t := timer.Start(ctx, store.planeId, "backup.S3Store.Fetch")
	defer t.Stop()
	fullPath := filepath.Join(store.prefix, remoteName)
	buf, err := store.s3.Download(fullPath, store.bucket)
	if err != nil {
		zap.L().Error("Failed to download file from s3 bucket", zap.String("s3_path", fullPath), zap.String("s3_bucket", store.bucket), zap.Error(err))
		return err
	}

	to, err := os.OpenFile(localFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer to.Close()
	_, err = to.Write(buf)
	if err != nil {
		zap.L().Error("Failed to write data to localfile with buf", zap.String("local_file", localFile), zap.Error(err))
		return err
	}
	fi, err := to.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file statistics for file: %v", err)
	}
	// TODO(mohit): See if the file has to be closed for statistics to be computed
	backupFileSize.WithLabelValues(fmt.Sprintf("%d", store.planeId), "backup.S3Store.Fetch").Observe(float64(fi.Size()))
	return nil
}

func (store *S3Store) Delete(ctx context.Context, remoteName string) error {
	_, t := timer.Start(ctx, store.planeId, "backup.S3Store.Delete")
	defer t.Stop()
	fullPath := filepath.Join(store.prefix, remoteName)
	err := store.s3.Delete(fullPath, store.bucket)
	if err != nil {
		zap.L().Error("Failed to delete file from s3 bucket", zap.String("s3_path", fullPath), zap.String("s3_bucket", store.bucket), zap.Error(err))
		return err
	}
	return nil
}

func (store *S3Store) ListFile(ctx context.Context, remotePrefix string) ([]string, error) {
	_, t := timer.Start(ctx, store.planeId, "backup.S3Store.ListFile")
	defer t.Stop()
	fullPath := filepath.Join(store.prefix, remotePrefix)
	s3ret, err := store.s3.ListFiles(store.bucket, fullPath, "")
	var ret []string
	if err != nil {
		zap.L().Error("Failed to list files in s3 bucket", zap.String("s3_path", fullPath), zap.String("s3_bucket", store.bucket), zap.Error(err))
		return ret, err
	}
	for _, item := range s3ret {
		ret = append(ret, strings.TrimPrefix(item, store.prefix+"/"))
	}
	return ret, nil
}

func NewLocalStore(path string, planeId ftypes.RealmID) (*LocalStore, error) {
	err := os.MkdirAll(path, 0777)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	return &LocalStore{backupPath: path, planeId: planeId}, nil
}

func (store *LocalStore) copyFile(fromName string, toName string) error {
	from, err := os.Open(fromName)
	if err != nil {
		return err
	}
	defer from.Close()

	to, err := os.OpenFile(toName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer to.Close()

	_, err = io.Copy(to, from)
	if err != nil {
		return err
	}
	return nil
}

func (store *LocalStore) Store(ctx context.Context, localFile string, remoteName string) error {
	_, t := timer.Start(ctx, store.planeId, "backup.LocalStore.ListFile")
	defer t.Stop()
	zap.L().Info("Storing local file to LocalStore", zap.String("local_file", localFile), zap.String("remote_name", remoteName))
	return store.copyFile(localFile, filepath.Join(store.backupPath, remoteName))
}

func (store *LocalStore) Delete(ctx context.Context, remoteName string) error {
	_, t := timer.Start(ctx, store.planeId, "backup.LocalStore.ListFile")
	defer t.Stop()
	fileName := filepath.Join(store.backupPath, remoteName)
	zap.L().Info("Deleting from LocalStore", zap.String("remote_name", remoteName))
	return os.Remove(fileName)
}

func (store *LocalStore) Exists(ctx context.Context, remoteName string) (bool, error) {
	_, t := timer.Start(ctx, store.planeId, "backup.LocalStore.ListFile")
	defer t.Stop()
	if _, err := os.Stat(filepath.Join(store.backupPath, remoteName)); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

func (store *LocalStore) Fetch(ctx context.Context, remoteName string, localFile string) error {
	_, t := timer.Start(ctx, store.planeId, "backup.LocalStore.ListFile")
	defer t.Stop()
	zap.L().Info("Fetching from LocalStore to local file", zap.String("local_file", localFile), zap.String("remote_name", remoteName))
	return store.copyFile(filepath.Join(store.backupPath, remoteName), localFile)
}

func (store *LocalStore) ListFile(ctx context.Context, remotePrefix string) ([]string, error) {
	_, t := timer.Start(ctx, store.planeId, "backup.LocalStore.ListFile")
	defer t.Stop()
	var fileNames []string
	err := filepath.Walk(store.backupPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		trimmedLocalName := strings.Trim(strings.TrimPrefix(path, store.backupPath), "/")
		if strings.HasPrefix(trimmedLocalName, remotePrefix) {
			fileNames = append(fileNames, trimmedLocalName)
		}
		return nil
	})
	return fileNames, err
}
