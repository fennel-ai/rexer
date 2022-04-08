package fbadger

import (
	"fennel/resource"
	"fmt"
	"path/filepath"

	"github.com/dgraph-io/badger/v3"
)

type DB struct {
	*badger.DB
	Config resource.Config
	resource.Scope
}

func (d DB) Close() error {
	return d.DB.Close()
}

func (d DB) Type() resource.Type {
	return resource.Badger
}

var _ resource.Resource = DB{}

type Config struct {
	Opts  badger.Options
	Scope resource.Scope
}

func (c Config) Materialize() (resource.Resource, error) {
	if !c.Opts.InMemory {
		c.Opts.Dir = filepath.Join(c.Opts.Dir, fmt.Sprintf("t_%d", c.Scope.ID()))
	}
	db, err := badger.Open(c.Opts)
	if err != nil {
		return nil, err
	}
	return DB{DB: db, Config: c, Scope: c.Scope}, nil
}

var _ resource.Config = Config{}
