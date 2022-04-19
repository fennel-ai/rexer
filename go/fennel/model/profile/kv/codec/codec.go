package codec

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

var (
	// Map from codec identifier to implementation.
	codecs = make(map[uint8]ProfileCodec)
)

type ProfileCodec interface {
	Identifier() uint8
	EncodeKey(otype ftypes.OType, oid string, key string) ([]byte, error)
	EncodeValue(version uint64, value value.Value) ([]byte, error)
	EagerDecode(buf []byte) (DecodedValue, error)
	LazyDecode(buf []byte) (DecodedValue, error)
}

type DecodedValue interface {
	UpdateTime() (uint64, error)
	Value() (value.Value, error)
}

func RegisterCodec(codec ProfileCodec) {
	codecs[codec.Identifier()] = codec
}

func GetCodec(id uint8) (ProfileCodec, error) {
	codec, ok := codecs[id]
	if !ok {
		return nil, fmt.Errorf("codec with id %v not registered", string(id))
	}
	return codec, nil
}
