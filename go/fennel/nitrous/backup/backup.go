package backup

import (
	"crypto/sha256"
	"fennel/lib/ftypes"
	"fmt"
	"github.com/gocarina/gocsv"
	"go.uber.org/zap"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type BackupManager struct {
	planeID ftypes.RealmID
	store   BackupStore
}

type uploadedManifestItem struct {
	LocalName       string `csv:"local_name"`
	Sha256          string `csv:"sha256_digest"`
	LocalModifyTime int64  `csv:"local_modtime"`
	FileSize        int64  `csv:"file_size"`
}

const manifestPrefix string = "manifest_"

func NewBackupManager(plainID ftypes.RealmID, store BackupStore) (*BackupManager, error) {
	return &BackupManager{planeID: plainID, store: store}, nil
}

func (bm *BackupManager) BackupCleanup(versionsToKeep []string) error {
	// The function clean up all the files that are not belong to the backups we want to keep
	if len(versionsToKeep) == 0 {
		return fmt.Errorf("can not keep 0 versions for safety purpose")
	}

	tempManifestFile, _ := ioutil.TempFile("", manifestPrefix)
	_ = tempManifestFile.Close() // need not the handle, just to use its name
	defer os.Remove(tempManifestFile.Name())

	sha256DigestsToKeep := map[string]struct{}{}
	// parse the manifest files for the versions we want to keep, and find out all files to keep
	for _, version := range versionsToKeep {
		err := bm.store.Fetch(manifestPrefix+version, tempManifestFile.Name())
		if err != nil {
			return fmt.Errorf("failed to download manifest %s", version)
		}

		var manifest []*uploadedManifestItem
		manifestFile, err := os.OpenFile(tempManifestFile.Name(), os.O_RDONLY, os.ModePerm)
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
		// delete the irrelevant data files from the remote
		sha256Digest := strings.TrimPrefix(fileName, "rawfile_")
		if _, keep := sha256DigestsToKeep[sha256Digest]; keep {
			zap.L().Info("Keeping file in backup store", zap.String("file_name", fileName))
			continue
		}

		zap.L().Info("Deleting from backup store", zap.String("file_name", fileName))
		err := bm.store.Delete(fileName)
		if err != nil {
			zap.L().Error("Deletion failed from backup store", zap.String("file_name", fileName), zap.Error(err))
		}
	}

	allRemoteFiles, err = bm.store.ListFile(manifestPrefix)
	if err != nil {
		return fmt.Errorf("failed to list files in the backup store: %w", err)
	}

	versionSet := map[string]struct{}{}
	for _, version := range versionsToKeep {
		versionSet[manifestPrefix+version] = struct{}{}
	}

	for _, fileName := range allRemoteFiles {
		// delete the irrelevant version manifests from the remote
		if _, keep := versionSet[fileName]; keep {
			zap.L().Info("Keeping file in backup store", zap.String("file_name", fileName))
			continue
		}
		zap.L().Info("Deleting from backup store", zap.String("file_name", fileName))
		err := bm.store.Delete(fileName)
		if err != nil {
			zap.L().Error("Deletion failed from backup store", zap.String("file_name", fileName), zap.Error(err))
		}
	}

	return nil
}

func (bm *BackupManager) ListBackups() ([]string, error) {
	manifestList, err := bm.store.ListFile(manifestPrefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list backups: %w", err)
	}

	var backupList []string
	for _, item := range manifestList {
		backupList = append(backupList, strings.TrimPrefix(item, manifestPrefix))
	}
	return backupList, nil
}

func (bm *BackupManager) BackupPath(dir string, versionName string) error {
	var uploadedManifest []*uploadedManifestItem
	var newManifest []*uploadedManifestItem

	zap.L().Info("Start to backup db into a versions", zap.String("local_dir", dir), zap.Uint32("plane", bm.planeID.Value()), zap.String("version", versionName))
	uploadedManifestFilename := dir + "/RexUploadedManifest.csv"
	uploadedManifestFile, err := os.OpenFile(uploadedManifestFilename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		zap.L().Warn("Unable to open current manifest file", zap.String("manifest_file", uploadedManifestFilename), zap.Error(err))
	} else {
		if err := gocsv.UnmarshalFile(uploadedManifestFile, &uploadedManifest); err != nil { // Load clients from file
			zap.L().Warn("Unable to parse current manifest file", zap.String("manifest_file", uploadedManifestFilename), zap.Error(err))
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
		zap.L().Error("Unable to walk directory", zap.String("directory", dir), zap.Error(err))
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

		itemFullName := filepath.Join(dir, item.LocalName)
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

	err = bm.store.Store(uploadedManifestTmpFilename, manifestPrefix+versionName)
	if err != nil {
		return fmt.Errorf("failed to upload the new manifest file %s: %w", uploadedManifestTmpFilename, err)
	}

	err = os.Rename(uploadedManifestTmpFilename, uploadedManifestFilename)
	if err != nil {
		return fmt.Errorf("failed to rewrite with the new manifest file %s: %w", uploadedManifestTmpFilename, err)
	}

	return nil
}

func DirIsEmpty(dir string) (bool, error) {
	fdir, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer fdir.Close()

	_, err = fdir.Readdirnames(1)
	if err == io.EOF {
		return true, err
	}
	return false, err
}

func (bm *BackupManager) RestoreToPath(dir string, versionName string) error {
	zap.L().Info("Starting to restore remote version to local path", zap.String("version", versionName), zap.String("local_dir", dir), zap.Uint32("plane", bm.planeID.Value()))

	folderEmpty, _ := DirIsEmpty(dir)
	if !folderEmpty {
		return fmt.Errorf("the path %s must be empty", dir)
	}

	// now it's clear that the directory is empty
	manifestFileName := dir + "/RexUploadedManifest.csv"
	err := bm.store.Fetch(manifestPrefix+versionName, manifestFileName)
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
		localDownloadedName := filepath.Join(dir, item.LocalName)
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

	manifestFileName = filepath.Join(dir, "_RexUploadedManifest.csv.tmp")
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

func (bm *BackupManager) RestoreLatest(dbDir string) error {
	backups, err := bm.ListBackups()
	if err != nil {
		return err
	}
	if len(backups) == 0 {
		zap.L().Warn("There is no previous backups")
		return nil
	}
	sort.Strings(backups)
	backupToRecover := backups[len(backups)-1]
	zap.L().Info("Going to restore the latest backup", zap.String("version", backupToRecover))
	err = bm.RestoreToPath(dbDir, backupToRecover)
	if err != nil {
		return err
	}
	zap.L().Info("Successfully restored the latest backup", zap.String("version", backupToRecover))
	return nil
}
