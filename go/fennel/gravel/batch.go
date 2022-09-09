package gravel

import (
	"errors"
)

var (
	ErrSealedBatch = errors.New("can not mutate a sealed batch")
)

type Batch struct {
	gravel    *Gravel
	entries   []Entry
	discarded bool
	sealed    bool
}

func (b *Batch) Set(k, v []byte, expires uint32) error {
	if b.sealed {
		return ErrSealedBatch
	}
	b.entries = append(b.entries, Entry{
		key: clonebytes(k),
		val: Value{
			data:    clonebytes(v),
			expires: Timestamp(expires),
			deleted: false,
		},
	})
	return nil
}

func (b *Batch) Del(k []byte) error {
	if b.sealed {
		return ErrSealedBatch
	}
	b.entries = append(b.entries, Entry{
		key: clonebytes(k),
		val: Value{deleted: true},
	})
	return nil
}

func (b *Batch) Discard() {
	b.discarded = true
	b.sealed = true
	b.entries = nil
}

func (b *Batch) Commit() error {
	if b.discarded {
		return errors.New("can not commit discarded commit batch")
	}
	err := b.gravel.commit(b)
	b.entries = nil
	return err
}

func (b *Batch) Entries() []Entry {
	return b.entries
}

func clonebytes(src []byte) []byte {
	dest := make([]byte, len(src))
	copy(dest, src)
	return dest
}
