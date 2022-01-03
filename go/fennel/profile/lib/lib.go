package lib

import (
	actionlog "fennel/actionlog/lib"
	"fennel/value"
)

type OType = actionlog.OType

type ProfileItem struct {
	Otype   OType
	Oid     uint64
	Key     string
	Version uint64
	Value   value.Value
}
type ProfileItemSer struct {
	Otype   OType
	Oid     uint64
	Key     string
	Version uint64
	Value   []byte
}
