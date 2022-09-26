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

	// TODO(mohit): Remove this or make this explicit that this is for testing purpose
	//
	// NOTE: This is purely for testing purpose - in the tests, we often want all the data to exist at rest.
	// In prod, this can still be used but involves an added latency due to table building and logging data to disk.
	// There it is okay to have some data in memory and gets lost as part of `Close()` or `TearDown()` since on
	// startup, the DB will sync by tailing from the last checkpoint.
	Flush() error
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

type apiModeKey struct{}
type apiMode string

const (
	Write apiMode = "write"
	Read  apiMode = "read"
)

func (m apiMode) String() string {
	return string(m)
}

func NewWriteContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, apiModeKey{}, Write)
}

func IsWrite(ctx context.Context) bool {
	m, ok := ctx.Value(apiModeKey{}).(apiMode)
	return ok && m == Write
}

func GetMode(ctx context.Context) apiMode {
	m, ok := ctx.Value(apiModeKey{}).(apiMode)
	if !ok {
		// We assume that default mode is read.
		return Read
	}
	return m
}
