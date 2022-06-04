package store

import (
	"time"
)

func ExpiryToTTL(expiry int64) (time.Duration, bool) {
	if expiry == 0 {
		return time.Duration(0), true
	}
	now := time.Now().Unix()
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
		ret[i] = buf[:n]
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
	for i, k := range vgs {
		n, err := enc.EncodeVal(buf, k)
		if err != nil {
			return nil, err
		}
		ret[i] = buf[:n]
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
		ret[i] = buf[:n]
		buf = buf[n:]
	}
	return ret, nil
}
