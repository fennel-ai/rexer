package main

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	"os"
)

var DB *sqlx.DB = nil

const DBNAME = "fennel.db"

func dbInit() {
	os.Remove(DBNAME)

	log.Printf("Creating db file %s...\n", DBNAME)
	file, err := os.Create(DBNAME)
	if err != nil {
		panic(err)
	}
	file.Close()
	log.Printf("%s created\n", DBNAME)

	DB, err = sqlx.Open("sqlite3", fmt.Sprintf("./%s", DBNAME))
	if err != nil {
		panic(err)
	}
	// now actually create the tables
	createCounterTables()
	createActionTable()
}

func dbShutdown() {
	DB.Close()
}
