package encoders

import (
	"fennel/hangar"
	"fennel/lib/utils/binary"
	"fmt"
)

type defaultEncoder struct{}

func Default() hangar.Encoder {
	return &defaultEncoder{}
}

func (d defaultEncoder) Codec() hangar.Codec {
	return hangar.Default
}

func (d defaultEncoder) EncodeKey(dest []byte, key hangar.Key) (int, error) {
	n, err := binary.PutBytes(dest, key.Data)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (d defaultEncoder) DecodeKey(src []byte, key *hangar.Key) (int, error) {
	data, n, err := binary.ReadBytes(src)
	if err != nil {
		return 0, err
	}
	key.Data = data
	return n, err
}

func (d defaultEncoder) EncodeVal(dest []byte, vg hangar.ValGroup) (int, error) {
	if !vg.Valid() {
		return 0, fmt.Errorf("invalid valgroup")
	}
	off := 0
	// first write the number of indices
	n, err := binary.PutUvarint(dest[off:], uint64(len(vg.Fields)))
	if err != nil {
		return 0, err
	}
	off += n
	// now write each index/value one at a time
	for i := range vg.Fields {
		if n, err = binary.PutBytes(dest[off:], vg.Fields[i]); err != nil {
			return 0, err
		}
		off += n
		if n, err = binary.PutBytes(dest[off:], vg.Values[i]); err != nil {
			return 0, err
		}
		off += n
	}
	// finally, write the expires
	// NOTE: it can be negative, so we use PutVarint, not PutUvarint
	n, err = binary.PutVarint(dest[off:], vg.Expiry)
	if err != nil {
		return 0, err
	}
	off += n
	return off, nil
}

func (d defaultEncoder) DecodeVal(src []byte, vg *hangar.ValGroup, reuse bool) (int, error) {
	off := 0
	numIndex, n, err := binary.ReadUvarint(src[off:])
	if err != nil {
		return 0, err
	}
	off += n
	if !reuse {
		dest := make([]byte, len(src))
		copy(dest, src)
		src = dest
	}
	vg.Fields = make(hangar.Fields, numIndex)
	vg.Values = make(hangar.Values, numIndex)

	for i := 0; i < int(numIndex); i++ {
		if vg.Fields[i], n, err = binary.ReadBytes(src[off:]); err != nil {
			return 0, err
		}
		off += n
		if vg.Values[i], n, err = binary.ReadBytes(src[off:]); err != nil {
			return 0, err
		}
		off += n
	}
	if vg.Expiry, n, err = binary.ReadVarint(src[off:]); err != nil {
		return 0, err
	}
	off += n
	return off, nil
}

func (d defaultEncoder) KeyLenHint(key hangar.Key) int {
	// upto 4 for the length of the data and the data itself
	return 4 + len(key.Data)
}

func (d defaultEncoder) ValLenHint(vg hangar.ValGroup) int {
	sz := 0
	for i := range vg.Fields {
		sz += 4 + len(vg.Fields[i]) // 4 bytes for length, plus length of field
		sz += 4 + len(vg.Values[i]) // 4 bytes for length, plus length of value
	}
	sz += 9 // upto 9 bytes for expires
	return sz
}

var _ hangar.Encoder = (*defaultEncoder)(nil)
