package backup

import (
	"crypto/sha256"
	"fennel/lib/ftypes"
	"fennel/s3"
	"fmt"
	"github.com/gocarina/gocsv"
	"go.uber.org/zap"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileStore interface {
	Store(localFile string, remoteName string) error
	Exist(remoteName string) (bool, error)
	Fetch(remoteName string, localFile string) error
	Delete(remoteName string) error
	ListFile(remotePrefix string) ([]string, error)
}

type S3Store struct {
	region string
	bucket string
	prefix string
	s3     *s3.Client
	logger *zap.Logger
}

type LocalStore struct {
	backupPath string
	logger     *zap.Logger
}

func NewS3Store(region string, bucketName string, dbInstanceName string, logger *zap.Logger) (*S3Store, error) {
	s3Client := s3.NewClient(s3.S3Args{Region: region})
	return &S3Store{region: region, bucket: bucketName, prefix: fmt.Sprintf("backups_v1/%s", dbInstanceName), s3: &s3Client, logger: logger}, nil
}

func (store *S3Store) Store(localFile string, remoteName string) error {
	from, err := os.Open(localFile)
	if err != nil {
		store.logger.Error(fmt.Sprintf("Failed to open %s for S3 upload, err: %v", localFile, err))
		return err
	}
	defer from.Close()
	fullPath := store.prefix + "/" + remoteName
	err = store.s3.Upload(from, fullPath, store.bucket)
	if err != nil {
		store.logger.Error(fmt.Sprintf("Failed to upload %s to s3 bucket %s, err: %v", fullPath, store.bucket, err))
	}
	return err
}

func (store *S3Store) Exist(remoteName string) (bool, error) {
	fullPath := store.prefix + "/" + remoteName
	return store.s3.Exist(fullPath, store.bucket)
}

func (store *S3Store) Fetch(remoteName string, localFile string) error {
	fullPath := store.prefix + "/" + remoteName
	buf, err := store.s3.Download(fullPath, store.bucket)
	if err != nil {
		store.logger.Error(fmt.Sprintf("Failed to download %s from s3 bucket %s, err: %v", fullPath, store.bucket, err))
		return err
	}

	to, err := os.OpenFile(localFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer to.Close()
	_, err = to.Write(buf)
	if err != nil {
		store.logger.Error(fmt.Sprintf("Failed to write data to localfile %s with buf of size %d, err: %v", localFile, len(buf), err))
	}
	return err
}

func (store *S3Store) Delete(remoteName string) error {
	fullPath := store.prefix + "/" + remoteName
	err := store.s3.Delete(fullPath, store.bucket)
	if err != nil {
		store.logger.Error(fmt.Sprintf("Failed to delete s3 data of %s in bucket %s, err: %v", fullPath, store.bucket, err))
	}
	return err
}

func (store *S3Store) ListFile(remotePrefix string) ([]string, error) {
	fullPath := store.prefix + "/" + remotePrefix
	s3ret, err := store.s3.ListFiles(store.bucket, fullPath)
	if err != nil {
		store.logger.Error(fmt.Sprintf("Failed to list s3 data of prefix %s in bucket %s, err: %v", fullPath, store.bucket, err))
	}
	var ret []string
	for _, item := range s3ret {
		ret = append(ret, strings.TrimPrefix(item, store.prefix+"/"))
	}
	return ret, err
}

func NewLocalStore(path string, logger *zap.Logger) (*LocalStore, error) {
	err := os.MkdirAll(path, 0777)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	return &LocalStore{backupPath: path, logger: logger}, nil
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
	store.logger.Info(fmt.Sprintf("Storing %s to file store %s", localFile, remoteName))
	return store.copyFile(localFile, store.backupPath+"/"+remoteName)
}

func (store *LocalStore) Delete(remoteName string) error {
	fileName := store.backupPath + "/" + remoteName
	store.logger.Info(fmt.Sprintf("Deleting %s from file store", fileName))
	return os.Remove(fileName)
}

func (store *LocalStore) Exist(remoteName string) (bool, error) {
	if _, err := os.Stat(store.backupPath + "/" + remoteName); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, err
	}
}

func (store *LocalStore) Fetch(remoteName string, localFile string) error {
	store.logger.Info(fmt.Sprintf("Fetching %s to local store %s", remoteName, localFile))
	return store.copyFile(store.backupPath+"/"+remoteName, localFile)
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

type BackupManager struct {
	planeID ftypes.RealmID
	logger  *zap.Logger
	store   FileStore
}

type uploadedManifestItem struct {
	LocalName       string `csv:"local_name"`
	Sha256          string `csv:"sha256_digest"`
	LocalModifyTime int64  `csv:"local_modtime"`
	FileSize        int64  `csv:"file_size"`
}

func NewBackupManager(plainID ftypes.RealmID, logger *zap.Logger, store FileStore) (*BackupManager, error) {
	return &BackupManager{planeID: plainID, logger: logger, store: store}, nil
}

func (bm *BackupManager) BackupCleanup(versionsToKeep []string) error {
	if len(versionsToKeep) == 0 {
		return fmt.Errorf("can not keep 0 versions for safety purpose")
	}

	tempfileName := fmt.Sprintf("/tmp/manifest_%d.tmp", time.Now().UnixNano())
	defer os.Remove(tempfileName)

	sha256DigestsToKeep := map[string]struct{}{}
	for _, version := range versionsToKeep {
		err := bm.store.Fetch("manifest_"+version, tempfileName)
		if err != nil {
			return fmt.Errorf("failed to download manifest %s", version)
		}

		var manifest []*uploadedManifestItem
		manifestFile, err := os.OpenFile(tempfileName, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return fmt.Errorf("unable to open the downloaded manifest file: %w", err)
		} else {
			if err := gocsv.UnmarshalFile(manifestFile, &manifest); err != nil { // Load clients from file
				_ = manifestFile.Close()
				return fmt.Errorf("unable to parse the downloaded manifest file: %w", err)
			}
			_ = manifestFile.Close()
		}
		for _, item := range manifest {
			sha256DigestsToKeep[item.Sha256] = struct{}{}
		}
	}

	allRemoteFiles, err := bm.store.ListFile("rawfile_")
	if err != nil {
		return fmt.Errorf("failed to list files in the backup store: %w", err)
	}

	for _, fileName := range allRemoteFiles {
		sha256Digest := strings.TrimPrefix(fileName, "rawfile_")
		if _, keep := sha256DigestsToKeep[sha256Digest]; keep == true {
			bm.logger.Info(fmt.Sprintf("Keeping file %s", fileName))
			continue
		}

		bm.logger.Info(fmt.Sprintf("Deleting file %s", fileName))
		err := bm.store.Delete(fileName)
		if err != nil {
			bm.logger.Error(fmt.Sprintf("Delete remote file %s failed: %v", fileName, err))
		}
	}

	allRemoteFiles, err = bm.store.ListFile("manifest_")
	if err != nil {
		return fmt.Errorf("failed to list files in the backup store: %w", err)
	}

	versionSet := map[string]struct{}{}
	for _, version := range versionsToKeep {
		versionSet["manifest_"+version] = struct{}{}
	}

	for _, fileName := range allRemoteFiles {
		if _, keep := versionSet[fileName]; keep {
			bm.logger.Info(fmt.Sprintf("Keeping file %s", fileName))
			continue
		}
		bm.logger.Info(fmt.Sprintf("Deleting file %s", fileName))
		err := bm.store.Delete(fileName)
		if err != nil {
			bm.logger.Error(fmt.Sprintf("Delete remote file %s failed: %v", fileName, err))
		}
	}

	return nil
}

func (bm *BackupManager) ListBackups() ([]string, error) {
	manifestList, err := bm.store.ListFile("manifest_")
	if err != nil {
		return nil, fmt.Errorf("failed to list backups: %w", err)
	}

	var backupList []string
	for _, item := range manifestList {
		backupList = append(backupList, strings.TrimPrefix(item, "manifest_"))
	}
	return backupList, nil
}

func (bm *BackupManager) BackupPath(dir string, versionName string) error {
	var uploadedManifest []*uploadedManifestItem
	var newManifest []*uploadedManifestItem

	bm.logger.Info(fmt.Sprintf("Start to backup db(%s) on plane %d into version: %s", dir, bm.planeID, versionName))
	uploadedManifestFilename := dir + "/RexUploadedManifest.csv"
	uploadedManifestFile, err := os.OpenFile(uploadedManifestFilename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		bm.logger.Warn(fmt.Sprintf("Unable to open current manifest file %s, with err: %v", uploadedManifestFilename, err))
	} else {
		if err := gocsv.UnmarshalFile(uploadedManifestFile, &uploadedManifest); err != nil { // Load clients from file
			bm.logger.Warn(fmt.Sprintf("Unable to parse the current manifest file %s, with err: %v", uploadedManifestFilename, err))
		}
		_ = uploadedManifestFile.Close()
	}

	var lastUploaded = map[string]*uploadedManifestItem{}
	for _, item := range uploadedManifest {
		lastUploaded[item.LocalName] = item
	}

	// Read last backup checkpoint manifest
	err = filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		trimmedLocalName := strings.Trim(strings.TrimPrefix(path, dir), "/")
		if (trimmedLocalName == "RexUploadedManifest.csv") || strings.HasSuffix(trimmedLocalName, ".tmp") {
			return nil
		}
		currentItem := &uploadedManifestItem{LocalName: trimmedLocalName, Sha256: "", LocalModifyTime: info.ModTime().UnixNano(), FileSize: info.Size()}
		newManifest = append(newManifest, currentItem)
		return nil
	})

	if err != nil {
		bm.logger.Warn(fmt.Sprintf("Unable to walk the directory %s, err: %v", dir, err))
		return err
	}

	for _, item := range newManifest {
		previousItem, ok := lastUploaded[item.LocalName]
		if ok {
			if (previousItem.FileSize == item.FileSize) && (previousItem.LocalModifyTime == item.LocalModifyTime) {
				item.Sha256 = previousItem.Sha256
				continue
			}
		}

		itemFullName := dir + "/" + item.LocalName
		f, err := os.Open(itemFullName)
		if err != nil {
			return err
		}

		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			_ = f.Close()
			return err
		}
		_ = f.Close()
		sha256Digest := fmt.Sprintf("%x", h.Sum(nil))

		item.Sha256 = sha256Digest
		// upload current item
		remoteName := "rawfile_" + sha256Digest
		err = bm.store.Store(itemFullName, remoteName)
		if err != nil {
			return fmt.Errorf("failed to upload the local file %s to remote %s: %w", itemFullName, remoteName, err)
		}
	}
	uploadedManifestTmpFilename := dir + "/_RexUploadedManifest.csv.tmp"
	uploadedManifestTmpFile, err := os.OpenFile(uploadedManifestTmpFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create the new manifest file %s: %w", uploadedManifestTmpFilename, err)
	}

	err = gocsv.MarshalFile(&newManifest, uploadedManifestTmpFile)
	if err != nil {
		return fmt.Errorf("failed to generate the new manifest file %s: %w", uploadedManifestTmpFilename, err)
	}

	err = uploadedManifestTmpFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close the new manifest file %s: %w", uploadedManifestTmpFilename, err)
	}

	err = bm.store.Store(uploadedManifestTmpFilename, "manifest_"+versionName)
	if err != nil {
		return fmt.Errorf("failed to upload the new manifest file %s: %w", uploadedManifestTmpFilename, err)
	}

	err = os.Rename(uploadedManifestTmpFilename, uploadedManifestFilename)
	if err != nil {
		return fmt.Errorf("failed to rewrite with the new manifest file %s: %w", uploadedManifestTmpFilename, err)
	}

	return nil
}

func (bm *BackupManager) RestoreToPath(dir string, versionName string) error {
	bm.logger.Info(fmt.Sprintf("Starting to restore remote version %s to local path %s on plane %d", versionName, dir, bm.planeID))
	fdir, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer fdir.Close()

	_, err = fdir.Readdirnames(1)
	if err == nil {
		return fmt.Errorf("The path %s must be empty", dir)
	} else if err != io.EOF {
		return err
	}
	// now it's clear that the directory is empty
	manifestFileName := dir + "/RexUploadedManifest.csv"
	err = bm.store.Fetch("manifest_"+versionName, manifestFileName)
	if err != nil {
		return err
	}

	var manifest []*uploadedManifestItem
	manifestFile, err := os.OpenFile(manifestFileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("unable to open the downloaded manifest file: %w", err)
	} else {
		if err := gocsv.UnmarshalFile(manifestFile, &manifest); err != nil { // Load clients from file
			_ = manifestFile.Close()
			return fmt.Errorf("unable to parse the downloaded manifest file: %w", err)
		}
		_ = manifestFile.Close()
	}

	for _, item := range manifest {
		localDownloadedName := dir + "/" + item.LocalName
		err := bm.store.Fetch("rawfile_"+item.Sha256, localDownloadedName)
		if err != nil {
			return fmt.Errorf("failed to download one of the file %s: %w", "rawfile_"+item.Sha256, err)
		}
		fstat, err := os.Stat(localDownloadedName)
		if err != nil {
			return fmt.Errorf("failed to get the stat of the downloaded file %s: %w", localDownloadedName, err)
		}
		item.FileSize = fstat.Size()
		item.LocalModifyTime = fstat.ModTime().UnixNano()
	}

	manifestFileName = dir + "/_RexUploadedManifest.csv.tmp"
	manifestFile, err = os.OpenFile(manifestFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to open the new manifest file %s: %w", manifestFileName, err)
	}
	err = gocsv.MarshalFile(&manifest, manifestFile)
	if err != nil {
		return fmt.Errorf("failed to generate the new manifest file %s: %w", manifestFileName, err)
	}
	err = manifestFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close the new manifest file %s: %w", manifestFileName, err)
	}
	err = os.Rename(manifestFileName, dir+"/RexUploadedManifest.csv")
	if err != nil {
		return fmt.Errorf("failed to overwrite with the new manifest file %s: %w", manifestFileName, err)
	}
	return nil
}

/*
func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s s3|local restore|backup|list|gc [restore_version]|[keep_version1 keep_version2 ...]\n", os.Args[0])
		return
	}
	logger := zap.NewExample()

	var store FileStore
	var err error

	if os.Args[1] == "s3" {
		store, err = NewS3Store("us-west-2", "nitrous-backup-test", "testdb", logger)
	} else if os.Args[1] == "local" {
		store, err = NewLocalStore("/tmp/dbbackup", logger)
	} else {
		panic("invalid store: " + os.Args[1])
	}

	if err != nil {
		panic(fmt.Sprintf("Failed to create file store dir %s", err))
	}

	bm, _ := NewBackupManager(12345678, logger, store)

	if os.Args[2] == "backup" {
		dir := "/tmp/testbadger"
		backupName := fmt.Sprintf("mainbackup_%d", time.Now().Unix())

		err = bm.BackupPath(dir, backupName)
		if err != nil {
			panic(fmt.Sprintf("Failed to create backup %s", err))
		}
		fmt.Printf("Backup Done!\n")
	} else if os.Args[2] == "restore" {
		if len(os.Args) < 4 {
			panic(fmt.Sprintf("Have to specify backup version"))
		}
		err = bm.RestoreToPath("/tmp/testbadger", os.Args[3])
		if err != nil {
			panic(fmt.Sprintf("Failed to restore backup %s", err))
		}
	} else if os.Args[2] == "list" {
		versions, err := bm.ListBackups()
		if err != nil {
			panic(fmt.Sprintf("Failed to list backups: %s", err))
		}
		for _, version := range versions {
			fmt.Println(version)
		}
	} else if os.Args[2] == "gc" {
		versions := make([]string, len(os.Args)-3)
		copy(versions[:], os.Args[3:])
		err := bm.BackupCleanup(versions)
		if err != nil {
			panic(fmt.Sprintf("Failed to clean up backups: %s", err))
		}
	} else {
		panic("invalid command: " + os.Args[2])
	}
}
*/
