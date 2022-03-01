package mothership

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestSchema(t *testing.T) {
	_, err := Create()
	assert.NoError(t, err)
}
