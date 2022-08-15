package billing

import (
	"testing"
)

func TestDBBasic(t *testing.T) {
	t.Parallel()
	t.Run("db_testIncrementOnlyQuery", func(t *testing.T) {
		testInsertAndQuery(t, dbProvider{})
	})
}
