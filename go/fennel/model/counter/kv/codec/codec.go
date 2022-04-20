package codec

import (
	"fmt"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

var (
	// Map from codec identifier to implementation.
	codecs = make(map[uint8]CounterCodec)
)

type CounterCodec interface {
	Identifier() uint8
	EncodeKey(aggId ftypes.AggId, bucket counter.Bucket) ([]byte, error)
	EncodeValue(value value.Value) ([]byte, error)
	DecodeValue(value []byte) (value.Value, error)
}

func RegisterCodec(codec CounterCodec) {
	codecs[codec.Identifier()] = codec
}

func GetCodec(id uint8) (CounterCodec, error) {
	codec, ok := codecs[id]
	if !ok {
		return nil, fmt.Errorf("codec with id %v not registered", string(id))
	}
	return codec, nil
}
