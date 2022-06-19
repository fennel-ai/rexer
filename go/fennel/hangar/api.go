package hangar

import (
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
	Encoder() Encoder
	GetMany(kgs []KeyGroup) ([]ValGroup, error)
	SetMany(keys []Key, vgs []ValGroup) error
	DelMany(keys []KeyGroup) error
	Close() error
	Teardown() error
	Backup(sink io.Writer, since uint64) (uint64, error)
	Restore(source io.Reader) error
}

type Codec uint8

const (
	None    Codec = 0
	Default Codec = 1
)
