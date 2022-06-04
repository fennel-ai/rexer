package encoders

import (
	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/store"
	"fmt"
)

type defaultEncoder struct{}

func Default() store.Encoder {
	return &defaultEncoder{}
}

func (d defaultEncoder) Codec() store.Codec {
	return store.Default
}

func (d defaultEncoder) EncodeKey(dest []byte, key store.Key) (int, error) {
	off := 0
	if len(dest) == 0 {
		return 0, fmt.Errorf("destination buffer is empty")
	}
	dest[off] = key.LShard
	off++
	n, err := binary.PutUvarint(dest[off:], uint64(key.TierID))
	if err != nil {
		return 0, err
	}
	off += n
	n, err = binary.PutBytes(dest[off:], key.Data)
	if err != nil {
		return 0, err
	}
	off += n
	return off, nil
}

func (d defaultEncoder) DecodeKey(src []byte, key *store.Key) (int, error) {
	if len(src) == 0 {
		return 0, fmt.Errorf("source buffer is empty")
	}
	key.LShard = src[0]
	off := 1
	tierID, n, err := binary.ReadUvarint(src[off:])
	if err != nil {
		return 0, err
	}
	off += n
	key.TierID = ftypes.RealmID(tierID)
	data, n, err := binary.ReadBytes(src[off:])
	if err != nil {
		return 0, err
	}
	off += n
	key.Data = data
	return n, err
}

func (d defaultEncoder) EncodeVal(dest []byte, vg store.ValGroup) (int, error) {
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

func (d defaultEncoder) DecodeVal(src []byte, vg *store.ValGroup, reuse bool) (int, error) {
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
	vg.Fields = make(store.Fields, numIndex)
	vg.Values = make(store.Values, numIndex)

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

func (d defaultEncoder) KeyLenHint(key store.Key) int {
	// 1 byte for the lshard, upto 10 for the tierid, upto 4 for the length of the data
	// and the data itself
	return 1 + 10 + 4 + len(key.Data)
}

func (d defaultEncoder) ValLenHint(vg store.ValGroup) int {
	sz := 0
	for i := range vg.Fields {
		sz += 4 + len(vg.Fields[i]) // 4 bytes for length, plus length of field
		sz += 4 + len(vg.Values[i]) // 4 bytes for length, plus length of value
	}
	sz += 9 // upto 9 bytes for expires
	return sz
}

var _ store.Encoder = (*defaultEncoder)(nil)
