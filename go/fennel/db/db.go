package db

import (
	"fennel/instance"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	"os"
)

var DB *sqlx.DB = nil

const dbname = "fennel.db"

func setup() error {
	log.Println("Setting up DB...")
	os.Remove(dbname)

	log.Printf("Creating db file %s...\n", dbname)
	file, err := os.Create(dbname)
	if err != nil {
		return err
	}
	file.Close()
	log.Printf("%s created\n", dbname)

	DB, err = sqlx.Open("sqlite3", fmt.Sprintf("./%s", dbname))
	if err != nil {
		return err
	}
	log.Printf("Done setting up DB\n")
	return nil
}

func init() {
	instance.Register(instance.DB, setup)
}
