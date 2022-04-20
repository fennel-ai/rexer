package impls

// This package is only created for registering different codec implementations.
// Any user of codec package should also import this package.
// Any new implementation of codec should be imported here to be used.
import (
	v1 "fennel/model/counter/kv/codec/impls/v1"
)

var (
	Current = v1.V1Codec{}
)
