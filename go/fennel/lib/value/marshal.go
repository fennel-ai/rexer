package value

import (
	"google.golang.org/protobuf/proto"
)

//func CaptainMarshal(v Value) ([]byte, error) {
//	arena := capnp.SingleSegment(nil)
//	msg, seg, _ := capnp.NewMessage(arena)
//
//	pa, err := ToCapnValue(v)
//	if err != nil {
//		return nil, err
//	}
//	return msg.Marshal()
//}
//
//func CaptainUnmarshal(data []byte, v *Value) error {
//	var pa PValue
//	if err := proto.Unmarshal(data, &pa); err != nil {
//		return err
//	}
//	a, err := FromProtoValue(&pa)
//	if err != nil {
//		return err
//	}
//	*v = a
//	return nil
//}

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
