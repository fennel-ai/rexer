package backup_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/samber/mo"

	"fennel/gravel"
	"fennel/hangar"
	"fennel/hangar/encoders"
	gravelDB "fennel/hangar/gravel"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/nitrous/backup"

	"github.com/stretchr/testify/assert"
)

func getData(numKey, numIndex int) ([]hangar.Key, []hangar.KeyGroup, []hangar.ValGroup) {
	keys := make([]hangar.Key, numKey)
	kgs := make([]hangar.KeyGroup, numKey)
	vgs := make([]hangar.ValGroup, numKey)
	fields := make(hangar.Fields, numIndex)
	for i := range fields {
		fields[i] = []byte(fmt.Sprintf("field%d", i))
	}
	for i := range keys {
		keys[i] = hangar.Key{Data: []byte(utils.RandString(10))}
		kgs[i] = hangar.KeyGroup{
			Prefix: keys[i],
			Fields: mo.Some(fields),
		}
		vgs[i] = hangar.ValGroup{
			Fields: fields,
		}
		for j := range kgs[i].Fields.OrEmpty() {
			vgs[i].Values = append(vgs[i].Values, []byte(fmt.Sprintf("value%d", i*numIndex+j)))
		}
	}
	return keys, kgs, vgs
}

// TODO(mohit): Create an integration test version with S3 storage instead of local storage

func TestBackupRestore(t *testing.T) {
	planeId := ftypes.RealmID(rand.Uint32())
	ctx := context.Background()

	dbDir := t.TempDir()
	fsDir := t.TempDir()
	numBackups := 6

	fs, _ := backup.NewLocalStore(fsDir, planeId)
	dm, _ := backup.NewBackupManager(planeId, fs, 1)

	// this is to validate later that the data was successfully backed up
	keyGroupByIt := make(map[int][][]hangar.KeyGroup, 6)
	valGroupByIt := make(map[int][][]hangar.ValGroup, 6)

	// Create 6 DBs => this is simulating creating backups at different timestamps
	for it := 0; it < numBackups; it++ {
		fmt.Printf("Creating DB in iteration %d/%d\n", it+1, numBackups)

		dbOpts := gravel.DefaultOptions().WithMaxTableSize(128 << 20).WithName("testdb")
		db, err := gravelDB.NewHangar(planeId, dbDir, &dbOpts, encoders.Default())
		assert.NoError(t, err)

		for j := 0; j < (6-it)*20000; j++ {
			// insert different number of rows in each time
			k, kg, vg := getData(1, 1)
			err := db.SetMany(ctx, k, vg)
			assert.NoError(t, err)
			keyGroupByIt[it] = append(keyGroupByIt[it], kg)
			valGroupByIt[it] = append(valGroupByIt[it], vg)
		}
		// flush with whatever we have - this is required for testing purposes. In real world, unflushed
		// entries will be read from the binlog and written again
		err = db.Flush()
		assert.NoError(t, err)

		// close to close the manifest file
		err = db.Close()
		assert.NoError(t, err)
		err = dm.BackupPath(ctx, dbDir, fmt.Sprintf("backup_name_%d", it))
		assert.NoError(t, err)
	}

	// create a new backup manager and check number of backups created (should same as above)
	{
		l, err := dm.ListBackups(ctx)
		assert.NoError(t, err)
		assert.Equal(t, len(l), numBackups)
	}

	for it := 0; it < numBackups; it++ {
		fmt.Printf("Verifying DB in iteration %d/%d\n", it+1, numBackups)

		err := os.RemoveAll(dbDir)
		assert.NoError(t, err)
		_ = os.Mkdir(dbDir, 0777)

		err = dm.RestoreToPath(ctx, dbDir, fmt.Sprintf("backup_name_%d", it))
		assert.NoError(t, err)

		dbOpts := gravel.DefaultOptions().WithMaxTableSize(128 << 20).WithName("testdb")
		db, err := gravelDB.NewHangar(planeId, dbDir, &dbOpts, encoders.Default())
		assert.NoError(t, err)

		// validate from the key groups and value groups loaded in the DB
		kgs := keyGroupByIt[it]
		vgs := valGroupByIt[it]
		for i, kg := range kgs {
			vg := vgs[i]
			actualVg, err := db.GetMany(ctx, kg)
			assert.NoError(t, err)
			assert.Equal(t, vg, actualVg)
		}

		_ = db.Close()
	}

	{
		fmt.Printf("Deleting backups: 1, 3, 5\n")
		err := dm.PurgeAllExceptVersions(ctx, []string{"backup_name_1", "backup_name_3", "backup_name_5"})
		assert.NoError(t, err)
	}

	for _, it := range []int{1, 3, 5} {
		fmt.Printf("Verifying again %d/%d\n", it, numBackups)

		err := os.RemoveAll(dbDir)
		assert.NoError(t, err)
		_ = os.Mkdir(dbDir, 0777)

		err = dm.RestoreToPath(ctx, dbDir, fmt.Sprintf("backup_name_%d", it))
		assert.NoError(t, err)

		dbOpts := gravel.DefaultOptions().WithMaxTableSize(128 << 20).WithName("testdb")
		db, err := gravelDB.NewHangar(planeId, dbDir, &dbOpts, encoders.Default())
		assert.NoError(t, err)

		kgs := keyGroupByIt[it]
		vgs := valGroupByIt[it]
		for i, kg := range kgs {
			vg := vgs[i]
			actualVg, err := db.GetMany(ctx, kg)
			assert.NoError(t, err)
			assert.Equal(t, vg, actualVg)
		}

		_ = db.Close()
	}
}
