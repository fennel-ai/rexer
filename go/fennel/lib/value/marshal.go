package value

import (
	"bytes"
	"capnproto.org/go/capnp/v3"
	"google.golang.org/protobuf/proto"
)

func CaptainMarshal(v Value) ([]byte, error) {
	_, bytes, err := ToCapnValue(v)
	return bytes, err
}

func CaptainUnmarshal(data []byte) (Value, error) {
	msg, _ := capnp.NewDecoder(bytes.NewBuffer(data)).Decode()
	cv, err := ReadRootCapnValue(msg)
	if err != nil {
		return nil, err
	}
	v, err := FromCapnValue(cv)
	if err != nil {
		return nil, err
	}
	return v, nil
}

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
	v, _, err := ParseValue(data, 0)
	return v, err
}
