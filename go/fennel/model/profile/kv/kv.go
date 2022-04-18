package kv

import (
	"context"
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/kvstore"
	"fennel/lib/profile"
	"fennel/lib/value"
	"fennel/model/profile/kv/codec"
	"fennel/model/profile/kv/codec/impls"
)

var (
	// Use "current" codec only for testing. Production tiers should have a
	// codec that is fixed at initialization.
	KeyEncoder = impls.Current
)

func Set(ctx context.Context, profiles []profile.ProfileItem, kv kvstore.ReaderWriter) error {
	for _, p := range profiles {
		k, err := KeyEncoder.EncodeKey(p.OType, p.Oid, p.Key)
		if err != nil {
			return fmt.Errorf("failed to encode key: %v", err)
		}
		// Get version of current value (if present) and compare with version
		// of incoming value.
		curr, err := kv.Get(ctx, k)
		if err != nil && err != kvstore.ErrKeyNotFound {
			return fmt.Errorf("failed to get current value: %v", err)
		}
		if err == nil {
			codec, err := codec.GetCodec(curr.Codec)
			if err != nil {
				return fmt.Errorf("failed to get codec: %v", err)
			}
			ev, err := codec.LazyDecode(curr.Raw)
			if err != nil {
				return fmt.Errorf("failed to decode current value: %v", err)
			}
			lastUpdated, err := ev.UpdateTime()
			if err != nil {
				return fmt.Errorf("failed to get current version: %v", err)
			}
			// We don't need to update the value if current version is already
			// larger than version of incoming value.
			if p.UpdateTime < lastUpdated {
				continue
			}
		}
		// Use latest codec to encode value.
		codec := impls.Current
		v, err := codec.EncodeValue(p.UpdateTime, p.Value)
		if err != nil {
			return fmt.Errorf("failed to encode value: %v", err)
		}
		err = kv.Set(ctx, k, kvstore.SerializedValue{
			Codec: codec.Identifier(),
			Raw:   v,
		})
		if err != nil {
			return fmt.Errorf("failed to set value in kv store: %v", err)
		}
	}
	return nil
}

func Get(ctx context.Context, profileKeys []profile.ProfileItemKey, kv kvstore.Reader) ([]profile.ProfileItem, error) {
	values := make([]profile.ProfileItem, len(profileKeys))
	for i, p := range profileKeys {
		values[i].OType = ftypes.OType(p.OType)
		values[i].Oid = p.Oid
		values[i].Key = p.Key
		k, err := KeyEncoder.EncodeKey(p.OType, p.Oid, p.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to encode key: %v", err)
		}
		v, err := kv.Get(ctx, k)
		if err == kvstore.ErrKeyNotFound {
			values[i].UpdateTime = 0
			values[i].Value = value.Nil
		} else if err != nil {
			return nil, fmt.Errorf("failed to get value: %v", err)
		} else {
			codec, err := codec.GetCodec(v.Codec)
			if err != nil {
				return nil, fmt.Errorf("failed to get codec: %v", err)
			}
			decodedValue, err := codec.EagerDecode(v.Raw)
			if err != nil {
				return nil, fmt.Errorf("failed to decode value: %v", err)
			}
			values[i].UpdateTime, _ = decodedValue.UpdateTime()
			values[i].Value, _ = decodedValue.Value()
		}
	}
	return values, nil
}
