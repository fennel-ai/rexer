package mothership

import (
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestSchema(t *testing.T) {
	hostname := "dw48w7ntx844.us-west-2.psdb.cloud"
	dbname := "controldb"
	username := "nehb2dtbg1hr"
	password := "pscale_pw_iOqKp0qepUAER8E-_APgydji6Ajj7fNQD7pfSseFYvg"

	connectStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?tls=true", username, password, hostname, dbname)
	DB, err := sqlx.Open("mysql", connectStr)
	defer DB.Close()
	assert.NoError(t, err)

	tables := []string{"schema_version",
		"customer", "tier", "data_plane", "eks", "kafka", "db", "memory_db", "elasticache"}
	for _, table := range tables {
		_, err := DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", table))
		assert.NoError(t, err)
	}

	_, err = Create(hostname, dbname, username, password)
	assert.NoError(t, err)
}
