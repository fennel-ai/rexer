package profile

import "testing"

func TestBadgerDB(t *testing.T) {
	t.Run("badger_basic", func(t *testing.T) {
		t.Parallel()
		testProviderBasic(t, badgerProvider{})
	})
	t.Run("badger_basic_version", func(t *testing.T) {
		t.Parallel()
		testProviderVersion(t, badgerProvider{})
	})
	t.Run("badger_set_batch", func(t *testing.T) {
		t.Parallel()
		testSetBatch(t, badgerProvider{})
	})
	t.Run("badger_get_version_batch", func(t *testing.T) {
		t.Parallel()
		testGetVersionBatched(t, badgerProvider{})
	})
	t.Run("badger_set_again", func(t *testing.T) {
		t.Parallel()
		testSetAgain(t, badgerProvider{})
	})
}
