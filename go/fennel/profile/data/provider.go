package data

import "fennel/profile/lib"

type Provider interface {
	Init() error
	Set(otype lib.OType, oid lib.OidType, key string, version uint64, valueSer []byte) error
	Get(otype lib.OType, oid lib.OidType, key string, version uint64) ([]byte, error)
	Name() string
}
