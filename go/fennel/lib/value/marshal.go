package value

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

const (
	// Codec values are stored in the first byte of the serialized value
	// and are used to determine the codec used to unmarshal the value.
	REXER_CODEC_V1 = 0x81
)

func ProtoMarshal(v Value) ([]byte, error) {
	pa, err := ToProtoValue(v)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pa)
}

func ProtoUnmarshal(data []byte, v *Value) error {
	var pa PValue
	if err := proto.Unmarshal(data, &pa); err != nil {
		return err
	}
	a, err := FromProtoValue(&pa)
	if err != nil {
		return err
	}
	*v = a
	return nil
}

func Unmarshal(data []byte) (Value, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data is empty")
	}
	if data[0] != REXER_CODEC_V1 {
		return nil, fmt.Errorf("unsupported codec: %x", data[0])
	}
	v, _, err := ParseValue(data[1:])
	return v, err
}

func Marshal(v Value) ([]byte, error) {
	ret, err := v.Marshal()
	return append([]byte{REXER_CODEC_V1}, ret...), err
}
