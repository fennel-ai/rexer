package v1

import (
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/lib/value"
	"fennel/model/counter/kv/codec"
)

func init() {
	codec.RegisterCodec(V1Codec{})
}

const id uint8 = 1

type V1Codec struct{}

var _ codec.CounterCodec = V1Codec{}

func (c V1Codec) Identifier() uint8 {
	return id
}

// defaultCodec key design is: codec | groupkey | window | width | index | aggregate_id
func (c V1Codec) EncodeKey(aggId ftypes.AggId, bucket counter.Bucket) ([]byte, error) {
	buf := make([]byte, 1+8+len(bucket.Key)+8+8+8+8) // codec + (len of key) + key + window + width + index + aggId
	cur := 0

	// Add a 1 byte prefix to indicate the codec used to encode the key.
	buf[cur] = id
	cur++

	if n, err := binary.PutString(buf[cur:], bucket.Key); err != nil {
		return nil, err
	} else {
		cur += n
	}
	if n, err := binary.PutUvarint(buf[cur:], uint64(bucket.Window)); err != nil {
		return nil, err
	} else {
		cur += n
	}
	if n, err := binary.PutUvarint(buf[cur:], bucket.Width); err != nil {
		return nil, err
	} else {
		cur += n
	}
	if n, err := binary.PutUvarint(buf[cur:], bucket.Index); err != nil {
		return nil, err
	} else {
		cur += n
	}
	if n, err := binary.PutUvarint(buf[cur:], uint64(aggId)); err != nil {
		return nil, err
	} else {
		cur += n
	}
	return buf[:cur], nil
}

func (c V1Codec) EncodeValue(v value.Value) ([]byte, error) {
	return value.Marshal(v)
}

func (c V1Codec) DecodeValue(v []byte) (value.Value, error) {
	var val value.Value
	if err := value.Unmarshal(v, &val); err != nil {
		return nil, err
	}
	return val, nil
}
