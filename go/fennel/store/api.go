package store

import (
	"fennel/lib/ftypes"
)

type Key struct {
	LShard byte
	TierID ftypes.RealmID
	Data   []byte
}

type Fields [][]byte
type Values [][]byte

type KeyGroup struct {
	Prefix Key
	Fields Fields
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

type Store interface {
	PlaneID() ftypes.RealmID
	Encoder() Encoder
	GetMany(kgs []KeyGroup) ([]ValGroup, error)
	SetMany(keys []Key, vgs []ValGroup) error
	DelMany(keys []KeyGroup) error
	Close() error
}

type Codec uint8

const (
	None    Codec = 0
	Default Codec = 1
)
