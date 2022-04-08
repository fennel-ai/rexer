package codex

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodex_Write(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		buf []byte
		c   Codex
		err bool
	}{
		{make([]byte, 5), Codex(5), false},
		{make([]byte, 1), Codex(9), false},
		{make([]byte, 0), Codex(9), true},
	}
	for _, scene := range scenarios {
		found, err := scene.c.Write(scene.buf)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, 1, found)
			assert.Equal(t, byte(scene.c), scene.buf[0])
		}
	}
}
