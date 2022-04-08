package test

import (
	"fennel/fbadger"
	"fennel/lib/ftypes"
	"fennel/resource"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/dgraph-io/badger/v3"
)

func defaultBadger(tierID ftypes.RealmID, dir string, memory bool) (fbadger.DB, error) {
	var opts badger.Options
	if !memory {
		dir, err := ioutil.TempDir(dir, fmt.Sprintf("badger-%d", tierID))
		if err != nil {
			return fbadger.DB{}, err
		}
		opts = badger.DefaultOptions(dir)
	} else {
		opts = badger.DefaultOptions("").WithInMemory(true)
	}
	opts = opts.WithLoggingLevel(badger.WARNING)
	conf := fbadger.Config{
		Opts:  opts,
		Scope: resource.NewTierScope(tierID),
	}
	ret, err := conf.Materialize()
	if err != nil {
		return fbadger.DB{}, err
	}
	return ret.(fbadger.DB), nil
}

func teardownBadger(db fbadger.DB) error {
	if err := db.Close(); err != nil {
		return err
	}
	conf := db.Config.(fbadger.Config)
	if !conf.Opts.InMemory {
		return os.RemoveAll(conf.Opts.Dir)
	}
	return nil
}
