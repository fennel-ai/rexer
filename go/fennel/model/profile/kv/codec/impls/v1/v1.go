package v1

import (
	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/lib/value"
	"fennel/model/profile/kv/codec"
)

// This package is imported in model/profile/kv/codec/impls/all.go to register.
func init() {
	codec.RegisterCodec(V1Codec{})
}

const id uint8 = 1

type V1Codec struct{}

var _ codec.ProfileCodec = V1Codec{}

func (c V1Codec) Identifier() uint8 {
	return id
}

func (c V1Codec) EncodeKey(otype ftypes.OType, oid uint64, key string) ([]byte, error) {
	keybuf := make([]byte, 8+len(otype)+8+8+len(key))

	cur := 0
	if n, err := binary.PutString(keybuf[cur:], string(otype)); err != nil {
		return nil, err
	} else {
		cur += n
	}

	if n, err := binary.PutUvarint(keybuf[cur:], oid); err != nil {
		return nil, err
	} else {
		cur += n
	}

	if n, err := binary.PutString(keybuf[cur:], key); err != nil {
		return nil, err
	} else {
		cur += n
	}

	return keybuf[:cur], nil
}

func (c V1Codec) EncodeValue(version uint64, value value.Value) ([]byte, error) {
	valueRaw, err := value.MarshalJSON()
	if err != nil {
		return nil, err
	}

	valbuf := make([]byte, 8+8+len(valueRaw))

	cur := 0

	// Write version first, then value.
	if n, err := binary.PutUvarint(valbuf[cur:], version); err != nil {
		return nil, err
	} else {
		cur += n
	}

	if n, err := binary.PutBytes(valbuf[cur:], valueRaw); err != nil {
		return nil, err
	} else {
		cur += n
	}

	return valbuf[:cur], nil
}

func (c V1Codec) EagerDecode(buf []byte) (codec.DecodedValue, error) {
	return newEagerlyDecodedValue(buf)
}

func (c V1Codec) LazyDecode(buf []byte) (codec.DecodedValue, error) {
	return newLazilyDecodedValue(buf), nil
}

type eagerlyDecodedValue struct {
	version uint64
	value   value.Value
}

var _ codec.DecodedValue = (*eagerlyDecodedValue)(nil)

func newEagerlyDecodedValue(buf []byte) (*eagerlyDecodedValue, error) {
	cur := 0

	version, n, err := binary.ReadUvarint(buf[cur:])
	if err != nil {
		return nil, err
	} else {
		cur += n
	}

	valueRaw, n, err := binary.ReadBytes(buf[cur:])
	if err != nil {
		return nil, err
	} else {
		cur += n //  nolint
	}

	val, err := value.FromJSON(valueRaw)
	if err != nil {
		return nil, err
	}

	decodedValue := eagerlyDecodedValue{
		version: version,
		value:   val,
	}

	return &decodedValue, nil
}

func (v *eagerlyDecodedValue) UpdateTime() (uint64, error) {
	return v.version, nil
}

func (v *eagerlyDecodedValue) Value() (value.Value, error) {
	return v.value, nil
}

// TODO: Use this in the profile kv model implementation.
type lazilyDecodedValue struct {
	raw     []byte
	idx     int
	version uint64
}

var _ codec.DecodedValue = (*lazilyDecodedValue)(nil)

func newLazilyDecodedValue(raw []byte) *lazilyDecodedValue {
	return &lazilyDecodedValue{raw: raw, idx: 0}
}

// This function only partially decodes the encoded value.
// This allows us to not decode the entire value if we don't need to - e.g. if
// the stored version is higher than the incoming version.
func (ev *lazilyDecodedValue) UpdateTime() (uint64, error) {
	if ev.idx > 0 {
		return ev.version, nil
	}
	version, n, err := binary.ReadUvarint(ev.raw)
	if err != nil {
		return 0, err
	} else {
		ev.version = version
		ev.idx += n
		return ev.version, nil
	}
}

func (ev *lazilyDecodedValue) Value() (value.Value, error) {
	// Extract the version from the encoded value if it hasn't been extracted
	// yet.
	if ev.idx == 0 {
		_, err := ev.UpdateTime()
		if err != nil {
			return value.Nil, err
		}
	}
	valueRaw, _, err := binary.ReadBytes(ev.raw[ev.idx:])
	if err != nil {
		return value.Nil, err
	} else {
		val, err := value.FromJSON(valueRaw)
		if err != nil {
			return value.Nil, err
		}
		return val, nil
	}
}
