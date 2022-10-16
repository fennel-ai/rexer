package hangar

import (
	"fmt"
	"time"

	"fennel/lib/utils/slice"

	"github.com/raulk/clock"
)

func ExpiryToTTL(expiry int64, clock clock.Clock) (time.Duration, bool) {
	if expiry == 0 {
		return time.Duration(0), true
	}
	now := clock.Now().Unix()
	if expiry < now {
		return time.Duration(0), false
	}
	return time.Duration(expiry-now) * time.Second, true
}

func EncodeKeyMany(keys []Key, enc Encoder) ([][]byte, error) {
	sz := 0
	for _, k := range keys {
		sz += enc.KeyLenHint(k)
	}
	buf := make([]byte, sz)
	ret := make([][]byte, len(keys))
	for i, k := range keys {
		n, err := enc.EncodeKey(buf, k)
		if err != nil {
			return nil, err
		}
		ret[i] = slice.Limit(buf[:n])
		buf = buf[n:]
	}
	return ret, nil
}

func EncodeValMany(vgs []ValGroup, enc Encoder) ([][]byte, error) {
	sz := 0
	for _, vg := range vgs {
		sz += enc.ValLenHint(vg)
	}
	buf := make([]byte, sz)
	ret := make([][]byte, len(vgs))
	for i, vg := range vgs {
		n, err := enc.EncodeVal(buf, vg)
		if err != nil {
			return nil, err
		}
		ret[i] = slice.Limit(buf[:n])
		buf = buf[n:]
	}
	return ret, nil
}

func EncodeKeyManyKG(kgs []KeyGroup, enc Encoder) ([][]byte, error) {
	sz := 0
	for _, kg := range kgs {
		sz += enc.KeyLenHint(kg.Prefix)
	}
	buf := make([]byte, sz)
	ret := make([][]byte, len(kgs))
	for i, kg := range kgs {
		n, err := enc.EncodeKey(buf, kg.Prefix)
		if err != nil {
			return nil, err
		}
		ret[i] = slice.Limit(buf[:n])
		buf = buf[n:]
	}
	return ret, nil
}

func MergeUpdates(keys []Key, updates []ValGroup) ([]Key, []ValGroup, error) {
	ptr := make(map[string]int, len(keys))
	n := 0
	for i, k := range keys {
		if j, ok := ptr[string(k.Data)]; ok {
			err := updates[j].Update(updates[i])
			if err != nil {
				return nil, nil, fmt.Errorf("failed to update valgroup: %w", err)
			}
		} else {
			ptr[string(k.Data)] = n
			keys[n] = k
			updates[n] = updates[i]
			n++
		}
	}
	keys = keys[:n]
	updates = updates[:n]
	return keys, updates, nil
}
