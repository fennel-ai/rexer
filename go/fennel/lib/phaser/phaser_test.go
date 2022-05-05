package phaser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadObjectss(t *testing.T) {
	readParquetFiles([]string{"asd"}, "ASd")
	assert.Equal(t, "asd", "asd1")
}
