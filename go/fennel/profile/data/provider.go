package data

type Provider interface {
	Init() error
	Set(otype uint32, oid uint64, key string, version uint64, valueSer []byte) error
	Get(otype uint32, oid uint64, key string, version uint64) ([]byte, error)
	Name() string
}
