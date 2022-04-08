package profile

import (
	"bytes"
	"context"
	"fennel/lib/codex"
	"fmt"

	"fennel/fbadger"
	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/utils/binary"
	"fennel/tier"

	"github.com/dgraph-io/badger/v3"
)

type badgerProvider struct{}

const (
	tablet   = fbadger.Profile
	default_ = codex.Codex(0)
)

func (b badgerProvider) set(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	return b.setBatch(ctx, tier, []profile.ProfileItemSer{
		{
			OType:   otype,
			Oid:     oid,
			Key:     key,
			Version: version,
			Value:   valueSer,
		},
	})
}

func (b badgerProvider) setBatch(ctx context.Context, tier tier.Tier, profiles []profile.ProfileItemSer) error {
	return tier.Badger.Update(func(txn *badger.Txn) error {
		for _, p := range profiles {
			if p.Version == 0 {
				return fmt.Errorf("profile version should be positive")
			}
			k, err := encodeBadgerKey(p.OType, p.Oid, p.Key, p.Version)
			if err != nil {
				return err
			}
			entry, err := txn.Get(k)
			switch err {
			case badger.ErrKeyNotFound:
				if err := txn.Set(k, p.Value); err != nil {
					return err
				}
			case nil:
				// no error => key exists, verify if the value is same or not
				err = entry.Value(func(v []byte) error {
					if bytes.Compare(v, p.Value) != 0 {
						return fmt.Errorf("profile key with the same version already exists")
					}
					return nil
				})
				if err != nil {
					return err
				}
			default:
				return err
			}
		}
		return nil
	})
}

func (b badgerProvider) get(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	if version > 0 {
		return b.getSpecificVersion(ctx, tier, otype, oid, key, version)
	}
	vid := versionIdentifier{otype: otype, oid: oid, key: key}
	maxVersionMap, err := b.getVersionBatched(ctx, tier, []versionIdentifier{vid})
	if err != nil {
		return nil, err
	}
	if version, ok := maxVersionMap[vid]; !ok {
		return nil, nil
	} else {
		return b.getSpecificVersion(ctx, tier, otype, oid, key, version)
	}
}

func (b badgerProvider) getSpecificVersion(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	if version == 0 {
		return nil, fmt.Errorf("version should be positive")
	}
	k, err := encodeBadgerKey(otype, oid, key, version)
	if err != nil {
		return nil, err
	}
	ret := []byte(nil)
	err = tier.Badger.View(func(txn *badger.Txn) error {
		entry, err := txn.Get(k)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return entry.Value(func(val []byte) error {
			ret = append(ret, val...)
			return nil
		})
	})
	return ret, err
}

func (b badgerProvider) getVersionBatched(ctx context.Context, tier tier.Tier, vids []versionIdentifier) (map[versionIdentifier]uint64, error) {
	ret := make(map[versionIdentifier]uint64, len(vids))
	err := tier.Badger.View(func(txn *badger.Txn) error {
		for _, vid := range vids {
			func(otype ftypes.OType, oid uint64, key string) {
				k, err := encodeBadgerKey(otype, oid, key, 0)
				if err != nil {
					return
				}
				iter := txn.NewIterator(badger.DefaultIteratorOptions)
				defer iter.Close()
				// iterate over all keys with the same prefix and find the largest version
				largest := uint64(0)
				for iter.Seek(k); iter.ValidForPrefix(k); iter.Next() {
					_, _, _, version, err := decodeBadgerKey(iter.Item().Key())
					if err == nil && version > largest {
						largest = version
					}
				}
				if largest > 0 {
					ret[vid] = largest
				}
			}(vid.otype, vid.oid, vid.key)
		}
		return nil
	})
	return ret, err
}

var _ provider = badgerProvider{}

func encodeBadgerKey(otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	// 1 byte for tablet, 1 for codex, upto 8 for version, upto 8 for oid, upto 8 each for length of otype/key for total of
	// 34 extra binary besides otype/okey (but in practice, we will need much less)
	keybuf := make([]byte, len(otype)+len(key)+34)

	// first write tablet, default codex, otype length (in varint), otype, oid (in varint), key length (in varint), key, version (in varint)
	cur := 0
	if n, err := tablet.Write(keybuf[cur:]); err != nil {
		return nil, err
	} else {
		cur += n
	}

	if n, err := default_.Write(keybuf[cur:]); err != nil {
		return nil, err
	} else {
		cur += n
	}

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

	if version > 0 {
		if n, err := binary.PutUvarint(keybuf[cur:], version); err != nil {
			return nil, err
		} else {
			cur += n
		}
	}
	return keybuf[:cur], nil
}

func decodeBadgerKey(buf []byte) (ftypes.OType, uint64, string, uint64, error) {
	cur := 0
	tbl, n, err := fbadger.ReadTablet(buf)
	if err != nil {
		return "", 0, "", 0, err
	}
	if tbl != tablet {
		return "", 0, "", 0, fmt.Errorf("badger key has wrong tablet: %v", tbl)
	}
	cur += n

	codec, n, err := codex.Read(buf[cur:])
	if err != nil {
		return "", 0, "", 0, err
	}
	if codec != default_ {
		return "", 0, "", 0, fmt.Errorf("badger key has wrong codex")
	}
	cur += n

	otype, n, err := binary.ReadString(buf[cur:])
	if err != nil {
		return "", 0, "", 0, err
	} else {
		cur += n
	}
	oid, n, err := binary.ReadUvarint(buf[cur:])
	if err != nil {
		return "", 0, "", 0, err
	} else {
		cur += n
	}
	key, n, err := binary.ReadString(buf[cur:])
	if err != nil {
		return "", 0, "", 0, err
	} else {
		cur += n
	}

	version, n, err := binary.ReadUvarint(buf[cur:])
	if err != nil {
		return "", 0, "", 0, err
	} else {
		cur += n
	}

	return ftypes.OType(otype), oid, key, version, nil
}
