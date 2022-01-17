package db

import (
	"fennel/instance"
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
	return droptable(t.Name, t.DB)
}

func (t Table) Type() resource.Type {
	return resource.DBTable
}

type TableConfig struct {
	SQL       string
	Name      string
	DB        Connection
	DropTable bool // if true, table is dropped before being recreated in TEST instance
}

func (conf TableConfig) Materialize() (resource.Resource, error) {
	if conf.DropTable && instance.Current() == instance.TEST {
		err := droptable(conf.Name, conf.DB)
		if err != nil {
			return nil, err
		}
	}
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

func droptable(name string, db Connection) error {
	statement, err := db.Prepare(fmt.Sprintf(
		`DROP TABLE IF EXISTS %s;`, name,
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
