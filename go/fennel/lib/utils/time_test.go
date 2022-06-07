package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
)

func TestDuration(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		w   ftypes.Window
		d   uint32
		err bool
	}{
		{ftypes.Window_MINUTE, 60, false},
		{ftypes.Window_HOUR, 3600, false},
		{ftypes.Window_DAY, 24 * 3600, false},
		{ftypes.Window(11), 0, true},
	}
	for _, scene := range scenarios {
		found, err := Duration(scene.w)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.d, found)
		}
	}
}
