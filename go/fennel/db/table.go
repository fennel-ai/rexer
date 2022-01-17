package db

import (
	"fennel/resource"
	"fmt"
)

type Table struct {
	Name string
	DB   Connection
}

var _ resource.Resource = Table{}

func (t Table) Close() error {
	return t.DB.Close()
}

func (t Table) Teardown() error {
	statement, err := t.DB.Prepare(fmt.Sprintf(
		`DROP TABLE %s IF EXISTS`, t.Name,
	))
	if err != nil {
		return err
	}
	_, err = statement.Exec()
	if err != nil {
		return err
	}
	return nil
}

func (t Table) Type() resource.Type {
	return resource.DBTable
}

type TableConfig struct {
	SQL  string
	Name string
	DB   Connection
}

func (conf TableConfig) Materialize() (resource.Resource, error) {
	statement, err := conf.DB.Prepare(conf.SQL)
	if err != nil {
		return nil, err
	}
	_, err = statement.Exec()
	if err != nil {
		return nil, err
	}
	return Table{conf.Name, conf.DB}, nil
}

var _ resource.Config = TableConfig{}
