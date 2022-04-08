package profile

import "testing"

func TestBadgerDB(t *testing.T) {
	t.Parallel()
	t.Run("badger_basic", func(t *testing.T) {
		testProviderBasic(t, badgerProvider{})
	})
	t.Run("badger_basic_version", func(t *testing.T) {
		testProviderVersion(t, badgerProvider{})
	})
	t.Run("badger_set_batch", func(t *testing.T) {
		testSetBatch(t, badgerProvider{})
	})
	t.Run("badger_get_version_batch", func(t *testing.T) {
		testGetVersionBatched(t, badgerProvider{})
	})
	t.Run("badger_set_again", func(t *testing.T) {
		testSetAgain(t, badgerProvider{})
	})
}
