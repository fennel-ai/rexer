package backup_test

import (
	"fennel/lib/ftypes"
	"fennel/nitrous/backup"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func generateKey(keySpace uint64) string {
	s := rand.Uint64() % keySpace
	return fmt.Sprintf("%d%d", s, s)
}

func TestBackupRestore(t *testing.T) {
	// below numbers are kind of arbitrary
	const kValueSize = 512
	const kMaxKeySpace = 500000
	value := make([]byte, 0, kValueSize)
	for i := 0; i < kValueSize; i++ {
		value = append(value, byte(i))
	}
	planeId := ftypes.RealmID(rand.Uint32())

	dbDir := t.TempDir()
	fsDir := t.TempDir()
	var trxId []uint64

	for it := 0; it < 6; it++ {
		fmt.Printf("Creating DB in iteration %d/6\n", it+1)
		fs, _ := backup.NewLocalStore(fsDir)
		dm, _ := backup.NewBackupManager(planeId, fs)

		db, err := badger.Open(badger.DefaultOptions(dbDir).WithBaseLevelSize(1024 * 512).WithMemTableSize(1024 * 1024).WithValueThreshold(1024))
		assert.NoError(t, err)

		for j := 0; j < (6-it)*20000; j++ {
			// insert different number of rows in each time
			batch := db.NewWriteBatch()
			key := generateKey(kMaxKeySpace)
			err := batch.Set([]byte(key), value)
			assert.NoError(t, err)
			err = batch.Flush()
			assert.NoError(t, err)
		}
		trxId = append(trxId, db.MaxVersion())
		err = db.Close()
		assert.NoError(t, err)
		err = dm.BackupPath(dbDir, fmt.Sprintf("backup_name_%d", it))
		assert.NoError(t, err)
	}

	{
		fs, _ := backup.NewLocalStore(fsDir)
		dm, _ := backup.NewBackupManager(planeId, fs)
		l, err := dm.ListBackups()
		assert.NoError(t, err)
		assert.Equal(t, len(l), 6)
	}

	for it := 0; it < 6; it++ {
		fmt.Printf("Verifying DB in iteration %d/6\n", it+1)
		fs, _ := backup.NewLocalStore(fsDir)
		dm, _ := backup.NewBackupManager(planeId, fs)

		err := os.RemoveAll(dbDir)
		assert.NoError(t, err)
		_ = os.Mkdir(dbDir, 0777)

		err = dm.RestoreToPath(dbDir, fmt.Sprintf("backup_name_%d", it))
		assert.NoError(t, err)

		db, err := badger.Open(badger.DefaultOptions(dbDir))
		assert.NoError(t, err)

		err = db.VerifyChecksum()
		assert.NoError(t, err)

		assert.Equal(t, db.MaxVersion(), trxId[it])
		_ = db.Close()
	}

	{
		fmt.Printf("Deleting some backups\n")
		fs, _ := backup.NewLocalStore(fsDir)
		dm, _ := backup.NewBackupManager(planeId, fs)
		err := dm.BackupCleanup([]string{"backup_name_1", "backup_name_3", "backup_name_5"})
		assert.NoError(t, err)
	}

	for idx, it := range []int{1, 3, 5} {
		fmt.Printf("Verifying again %d/3\n", idx+1)
		fs, _ := backup.NewLocalStore(fsDir)
		dm, _ := backup.NewBackupManager(planeId, fs)

		err := os.RemoveAll(dbDir)
		assert.NoError(t, err)
		_ = os.Mkdir(dbDir, 0777)

		err = dm.RestoreToPath(dbDir, fmt.Sprintf("backup_name_%d", it))
		assert.NoError(t, err)

		db, err := badger.Open(badger.DefaultOptions(dbDir))
		assert.NoError(t, err)

		err = db.VerifyChecksum()
		assert.NoError(t, err)

		assert.Equal(t, db.MaxVersion(), trxId[it])
		_ = db.Close()
	}
}
