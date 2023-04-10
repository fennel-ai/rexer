package mothership

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"fennel/lib/ftypes"
	"fennel/resource"

	"github.com/jmoiron/sqlx"
)

const (
	testLogicalDBName = "controldb"
)

func mothershipDbConfig() (string, string, string) {
	return os.Getenv("MYSQL_USERNAME"), os.Getenv("MYSQL_PASSWORD"), os.Getenv("MYSQL_SERVER_ADDRESS")
}

func NewTestMothership() (mothership Mothership, err error) {
	rand.Seed(time.Now().UnixNano())
	mothershipID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewMothershipScope(mothershipID)
	dbname := scope.PrefixedName(testLogicalDBName)
	err = Setup(mothershipID)
	if err != nil {
		return mothership, fmt.Errorf("error setting up db: %v", err)
	}
	testUsername, testPassword, testHostname := mothershipDbConfig()
	return CreateFromArgs(&MothershipArgs{
		MothershipID:  mothershipID,
		MysqlHost:     testHostname,
		MysqlDB:       dbname,
		MysqlUsername: testUsername,
		MysqlPassword: testPassword,
	})
}

func ClearTestTables(DB *sqlx.DB) error {
	tables := []string{"schema_version", "customer", "tier", "data_plane",
		"eks", "kafka", "db", "memory_db", "elasticache", "launch_request", "launch_history"}
	for _, table := range tables {
		_, err := DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", table))
		if err != nil {
			return err
		}
	}
	return nil
}

func Setup(mID ftypes.RealmID) error {
	scope := resource.NewMothershipScope(mID)
	dbname := scope.PrefixedName(testLogicalDBName)
	testUsername, testPassword, testHostname := mothershipDbConfig()
	connectStr := fmt.Sprintf("%s:%s@tcp(%s)/",
		testUsername, testPassword, testHostname)
	DB, err := sqlx.Open("mysql", connectStr)
	if err != nil {
		return err
	}
	defer DB.Close()

	if _, err = DB.Exec("CREATE DATABASE IF NOT EXISTS " + dbname); err != nil {
		return err
	}
	if _, err = DB.Exec("USE " + dbname); err != nil {
		return err
	}
	err = ClearTestTables(DB)
	return err
}

func Teardown(m Mothership) error {
	scope := resource.NewMothershipScope(m.ID)
	dbname := scope.PrefixedName(testLogicalDBName)
	testUsername, testPassword, testHostname := mothershipDbConfig()
	connectStr := fmt.Sprintf("%s:%s@tcp(%s)/%s",
		testUsername, testPassword, testHostname, dbname)
	DB, err := sqlx.Open("mysql", connectStr)
	if err != nil {
		return err
	}
	defer DB.Close()

	_, err = DB.Exec("DROP DATABASE IF EXISTS " + dbname)
	return err
}
