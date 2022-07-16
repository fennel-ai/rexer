package hangar

import (
	"context"
	"io"

	"fennel/lib/ftypes"

	"github.com/samber/mo"
)

type Key struct {
	Data []byte
}

type Fields [][]byte
type Values [][]byte

type KeyGroup struct {
	Prefix Key
	// If Fields is absent, all fields are selected.
	Fields mo.Option[Fields]
}

type ValGroup struct {
	Expiry int64
	Fields Fields
	Values Values
}

type Result struct {
	Ok  ValGroup
	Err error
}

type Encoder interface {
	Codec() Codec
	EncodeKey(dest []byte, key Key) (int, error)
	DecodeKey(src []byte, key *Key) (int, error)
	KeyLenHint(key Key) int
	EncodeVal(dest []byte, vg ValGroup) (int, error)
	DecodeVal(src []byte, vg *ValGroup, reuse bool) (int, error)
	ValLenHint(vg ValGroup) int
}

type Hangar interface {
	PlaneID() ftypes.RealmID
	GetMany(ctx context.Context, kgs []KeyGroup) ([]ValGroup, error)
	SetMany(ctx context.Context, keys []Key, vgs []ValGroup) error
	DelMany(ctx context.Context, keys []KeyGroup) error
	Close() error
	Teardown() error
	Backup(sink io.Writer, since uint64) (uint64, error)
	Restore(dbDir string) error
}

type Reader interface {
	PlaneID() ftypes.RealmID
	GetMany(ctx context.Context, kgs []KeyGroup) ([]ValGroup, error)
}

type Codec uint8

const (
	None    Codec = 0
	Default Codec = 1
)
