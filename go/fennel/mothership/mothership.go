package mothership

import (
	"fmt"
	"log"

	"fennel/db"
	"fennel/resource"
)

type Mothership struct {
	DB db.Connection
}

func Create(hostname, dbname, username, password string) (mothership Mothership, err error) {
	log.Print("Connecting to mysql")
	mysqlConfig := db.MySQLConfig{
		Host:     hostname,
		DBname:   dbname,
		Username: username,
		Password: password,
		Schema:   Schema,
		Scope:    resource.NewMothershipScope(1),
	}
	sqlConn, err := mysqlConfig.Materialize()
	if err != nil {
		return mothership, fmt.Errorf("failed to connect with mysql: %v", err)
	}
	return Mothership{
		DB: sqlConn.(db.Connection),
	}, nil
}
