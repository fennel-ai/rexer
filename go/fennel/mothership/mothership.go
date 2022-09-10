package mothership

import (
	"fmt"
	"log"

	"fennel/db"
	"fennel/lib/ftypes"
	"fennel/resource"
)

type MothershipArgs struct {
	MothershipID       ftypes.RealmID `arg:"--mothership_id,env:MOTHERSHIP_ID"`
	MysqlHost          string         `arg:"--mothership_mysql_host,env:MOTHERSHIP_MYSQL_ADDRESS"`
	MysqlDB            string         `arg:"--mothership_mysql_db,env:MOTHERSHIP_MYSQL_DBNAME"`
	MysqlUsername      string         `arg:"--mothership_mysql_user,env:MOTHERSHIP_MYSQL_USERNAME"`
	MysqlPassword      string         `arg:"--mothership_mysql_password,env:MOTHERSHIP_MYSQL_PASSWORD"`
	MothershipEndpoint string         `arg:"--mothership_endpoint,env:MOTHERSHIP_ENDPOINT"`
}

type Mothership struct {
	ID       ftypes.RealmID
	DB       db.Connection
	Endpoint string
}

func CreateFromArgs(args *MothershipArgs) (mothership Mothership, err error) {
	mothershipID := args.MothershipID
	scope := resource.NewMothershipScope(mothershipID)

	log.Print("Connecting to mysql")
	mysqlConfig := db.MySQLConfig{
		Host:     args.MysqlHost,
		DBname:   args.MysqlDB,
		Username: args.MysqlUsername,
		Password: args.MysqlPassword,
		Schema:   Schema,
		Scope:    scope,
	}
	sqlConn, err := mysqlConfig.Materialize()
	if err != nil {
		return mothership, fmt.Errorf("failed to connect with mysql: %v", err)
	}
	return Mothership{
		ID:       mothershipID,
		DB:       sqlConn.(db.Connection),
		Endpoint: args.MothershipEndpoint,
	}, nil
}
