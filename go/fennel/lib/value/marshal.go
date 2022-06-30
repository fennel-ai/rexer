package value

import (
	"bytes"
	"capnproto.org/go/capnp/v3"
)

func CaptainMarshal(v Value) ([]byte, error) {
	_, bytes, err := ToCapnValue(v)
	return bytes, err
}

func CaptainUnmarshal(data []byte) (Value, error) {
	msg, _ := capnp.NewDecoder(bytes.NewBuffer(data)).Decode()
	cv, err := ReadRootCapnValue(msg)
	//return nil, err
	if err != nil {
		return nil, err
	}
	v, err := FromCapnValue(cv)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func Marshal(v Value) ([]byte, error) {
	pa, err := ToProtoValue(v)
	if err != nil {
		return nil, err
	}
	return pa.MarshalVT()
}

func Unmarshal(data []byte, v *Value) error {
	//var pa PValue
	pa := PValueFromVTPool()
	if err := pa.UnmarshalVT(data); err != nil {
		return err
	}
	a, err := FromProtoValue(pa)
	pa.ReturnToVTPool()
	if err != nil {
		return err
	}
	*v = a
	return nil
}
