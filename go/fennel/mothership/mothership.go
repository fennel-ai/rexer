package mothership

import (
	"fmt"
	"log"

	"fennel/db"
	"fennel/resource"
	"github.com/jmoiron/sqlx"
)

const (
	defaultHostName = "dw48w7ntx844.us-west-2.psdb.cloud"
	defaultDBName   = "controldb"
	defaultUserName = "nehb2dtbg1hr"
	defaultPassword = "pscale_pw_iOqKp0qepUAER8E-_APgydji6Ajj7fNQD7pfSseFYvg"
)

type Mothership struct {
	DB db.Connection
}

func Create() (Mothership, error) {
	log.Print("Connecting to mysql")
	err := ClearTables()
	if err != nil {
		return Mothership{}, err
	}
	mysqlConfig := db.MySQLConfig{
		Host:     defaultHostName,
		DBname:   defaultDBName,
		Username: defaultUserName,
		Password: defaultPassword,
		Schema:   Schema,
		Scope:    resource.NewMothershipScope(1),
	}
	sqlConn, err := mysqlConfig.Materialize()
	if err != nil {
		return Mothership{}, fmt.Errorf("failed to connect with mysql: %v", err)
	}
	return Mothership{
		DB: sqlConn.(db.Connection),
	}, nil
}

func ClearTables() error {
	connectStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?tls=true",
		defaultUserName, defaultPassword, defaultHostName, defaultDBName)
	DB, err := sqlx.Open("mysql", connectStr)
	if err != nil {
		return err
	}
	defer DB.Close()

	tables := []string{"schema_version",
		"customer", "tier", "data_plane", "eks", "kafka", "db", "memory_db", "elasticache", "launch_request"}
	for _, table := range tables {
		_, err := DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", table))
		if err != nil {
			return err
		}
	}
	return nil
}
