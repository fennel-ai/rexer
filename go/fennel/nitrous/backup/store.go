package backup

import (
	"fennel/s3"
	"fmt"
	"go.uber.org/zap"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type BackupStore interface {
	Store(localFile string, remoteName string) error
	Exists(remoteName string) (bool, error)
	Fetch(remoteName string, localFile string) error
	Delete(remoteName string) error
	ListFile(remotePrefix string) ([]string, error)
}

type S3Store struct {
	region string
	bucket string
	prefix string
	s3     *s3.Client
}

type LocalStore struct {
	backupPath string
}

func NewS3Store(region string, bucketName string, dbInstanceName string) (*S3Store, error) {
	s3Client := s3.NewClient(s3.S3Args{Region: region})
	return &S3Store{region: region, bucket: bucketName, prefix: fmt.Sprintf("backups_v1/%s", dbInstanceName), s3: &s3Client}, nil
}

func (store *S3Store) Store(localFile string, remoteName string) error {
	from, err := os.Open(localFile)
	if err != nil {
		zap.L().Error("Failed to open local file for S3 upload", zap.String("local_file", localFile), zap.Error(err))
		return err
	}
	defer from.Close()
	fullPath := filepath.Join(store.prefix, remoteName)
	err = store.s3.Upload(from, fullPath, store.bucket)
	if err != nil {
		zap.L().Error("Failed to upload local file to s3 bucket", zap.String("s3_path", fullPath), zap.String("s3_bucket", store.bucket), zap.Error(err))
		return err
	}
	return nil
}

func (store *S3Store) Exists(remoteName string) (bool, error) {
	fullPath := filepath.Join(store.prefix, remoteName)
	return store.s3.Exists(fullPath, store.bucket)
}

func (store *S3Store) Fetch(remoteName string, localFile string) error {
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
	return nil
}

func (store *S3Store) Delete(remoteName string) error {
	fullPath := filepath.Join(store.prefix, remoteName)
	err := store.s3.Delete(fullPath, store.bucket)
	if err != nil {
		zap.L().Error("Failed to delete file from s3 bucket", zap.String("s3_path", fullPath), zap.String("s3_bucket", store.bucket), zap.Error(err))
		return err
	}
	return nil
}

func (store *S3Store) ListFile(remotePrefix string) ([]string, error) {
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

func NewLocalStore(path string) (*LocalStore, error) {
	err := os.MkdirAll(path, 0777)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	return &LocalStore{backupPath: path}, nil
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

func (store *LocalStore) Store(localFile string, remoteName string) error {
	zap.L().Info("Storing local file to LocalStore", zap.String("local_file", localFile), zap.String("remote_name", remoteName))
	return store.copyFile(localFile, filepath.Join(store.backupPath, remoteName))
}

func (store *LocalStore) Delete(remoteName string) error {
	fileName := filepath.Join(store.backupPath, remoteName)
	zap.L().Info("Deleting from LocalStore", zap.String("remote_name", remoteName))
	return os.Remove(fileName)
}

func (store *LocalStore) Exists(remoteName string) (bool, error) {
	if _, err := os.Stat(filepath.Join(store.backupPath, remoteName)); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

func (store *LocalStore) Fetch(remoteName string, localFile string) error {
	zap.L().Info("Fetching from LocalStore to local file", zap.String("local_file", localFile), zap.String("remote_name", remoteName))
	return store.copyFile(filepath.Join(store.backupPath, remoteName), localFile)
}

func (store *LocalStore) ListFile(remotePrefix string) ([]string, error) {
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
