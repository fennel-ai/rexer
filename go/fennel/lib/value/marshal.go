package value

import (
	"capnproto.org/go/capnp/v3"
	"google.golang.org/protobuf/proto"
)

func CaptainMarshal(v Value) ([]byte, error) {
	_, ret, err := ToCapnValue(v)
	if err != nil {
		return nil, err
	}
	return ret, err
}

func CaptainUnmarshal(data []byte, v *Value) error {
	var pa CapnValue
	msg, err = capnp.Unmarshal(data)

	if err := proto.Unmarshal(data, &pa); err != nil {
		return err
	}
	a, err := FromCapnValue(pa)
	if err != nil {
		return err
	}
	*v = a
	return nil
}

func Marshal(v Value) ([]byte, error) {
	pa, err := ToProtoValue(v)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pa)
}

func Unmarshal(data []byte, v *Value) error {
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
