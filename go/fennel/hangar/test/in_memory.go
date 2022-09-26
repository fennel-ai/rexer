package test

import (
	"context"
	"io"
	"sync"

	"fennel/hangar"
	"fennel/lib/ftypes"
)

// In-memory implementation of the Hangar interface.
// Note that this implementation does not perform key-expiry.
type InMemoryHangar struct {
	planeId ftypes.RealmID
	m       map[string]map[string]string

	mu sync.RWMutex
}

func (h *InMemoryHangar) Flush() error {
	//TODO implement me
	panic("implement me")
}

func NewInMemoryHangar(planeId ftypes.RealmID) *InMemoryHangar {
	return &InMemoryHangar{
		planeId: planeId,
		m:       make(map[string]map[string]string),
	}
}

var _ hangar.Hangar = &InMemoryHangar{}

func (h *InMemoryHangar) PlaneID() ftypes.RealmID {
	return h.planeId
}

func (h *InMemoryHangar) GetMany(ctx context.Context, kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var vgs []hangar.ValGroup
	for _, kg := range kgs {
		vg := hangar.ValGroup{}
		if val, ok := h.m[string(kg.Prefix.Data)]; ok {
			if kg.Fields.IsAbsent() {
				for k, v := range val {
					vg.Fields = append(vg.Fields, []byte(k))
					vg.Values = append(vg.Values, []byte(v))
				}
			} else {
				for _, f := range kg.Fields.MustGet() {
					if v, ok := val[string(f)]; ok {
						vg.Fields = append(vg.Fields, f)
						vg.Values = append(vg.Values, []byte(v))
					}
				}
			}
		}
		vgs = append(vgs, vg)
	}
	return vgs, nil
}

func (h *InMemoryHangar) SetMany(ctx context.Context, keys []hangar.Key, vgs []hangar.ValGroup) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	keys, vgs, err := hangar.MergeUpdates(keys, vgs)
	if err != nil {
		return err
	}
	for i, key := range keys {
		vg := vgs[i]
		if _, ok := h.m[string(key.Data)]; !ok {
			h.m[string(key.Data)] = make(map[string]string)
		}
		for j, f := range vg.Fields {
			h.m[string(key.Data)][string(f)] = string(vg.Values[j])
		}
	}
	return nil
}

func (h *InMemoryHangar) DelMany(ctx context.Context, keys []hangar.KeyGroup) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, key := range keys {
		k := string(key.Prefix.Data)
		if key.Fields.IsAbsent() {
			delete(h.m, k)
		} else {
			for _, f := range key.Fields.MustGet() {
				delete(h.m[k], string(f))
			}
		}
	}
	return nil
}
func (h *InMemoryHangar) Close() error                                        { return nil }
func (h *InMemoryHangar) Teardown() error                                     { return nil }
func (h *InMemoryHangar) Backup(sink io.Writer, since uint64) (uint64, error) { return 0, nil }
func (h *InMemoryHangar) Restore(source io.Reader) error                      { return nil }
