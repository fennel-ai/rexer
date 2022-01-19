package data

import (
	"fennel/redis"
	"fennel/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

type mockProvider struct {
	ret []byte
}

func (m *mockProvider) change(n []byte) {
	m.ret = n
}
func (m *mockProvider) Init() error { return nil }
func (m *mockProvider) Set(otype uint32, oid uint64, key string, version uint64, valueSer []byte) error {
	return nil
}
func (m *mockProvider) Get(otype uint32, oid uint64, key string, version uint64) ([]byte, error) {
	return m.ret, nil
}
func (m *mockProvider) Name() string { return "mock_provider" }

var _ Provider = &mockProvider{}

func TestCachedDBBasic(t *testing.T) {
	DB, err := test.DefaultDB()
	assert.NoError(t, err)

	table, err := NewProfileTable(DB)
	assert.NoError(t, err)

	client, err := redis.DefaultClient()
	assert.NoError(t, err)
	defer client.Close()
	p := CachedDB{cache: redis.NewCache(client.(redis.Client)), groundTruth: table}
	testProviderBasic(t, p)
}

func TestCaching(t *testing.T) {
	// test that we cache the value instead of always pulling from ground truth
	client, err := redis.DefaultClient()
	assert.NoError(t, err)
	defer client.Close()
	origmock := []byte{1, 2, 3}
	gt := mockProvider{origmock}
	p := CachedDB{cache: redis.NewCache(client.(redis.Client)), groundTruth: &gt}
	err = p.Init()
	assert.NoError(t, err)

	// initially we should get the mocked origmock value back
	found, err := p.Get(1, 1232, "summary", 1)
	assert.Equal(t, origmock, found)

	// now change the mocked value
	newmock := []byte{4, 5}
	gt.change(newmock)

	// we should still get origmock back because it's in cache
	found, err = p.Get(1, 1232, "summary", 1)
	assert.NoError(t, err)
	assert.Equal(t, origmock, found)

	// but if we set a new value, we will delete the key (remember: we don't fill cache on sets)
	err = p.Set(1, 1232, "summary", 1, []byte{7, 8, 9})
	assert.NoError(t, err)

	// so subsequent gets should get the new updated mock back
	found, err = p.Get(1, 1232, "summary", 1)
	assert.Equal(t, newmock, found)
}

func TestCachedDBVersion(t *testing.T) {
	DB, err := test.DefaultDB()
	assert.NoError(t, err)
	table, err := NewProfileTable(DB)
	assert.NoError(t, err)

	client, err := redis.DefaultClient()
	assert.NoError(t, err)
	defer client.Close()

	p := CachedDB{cache: redis.NewCache(client.(redis.Client)), groundTruth: table}
	testProviderVersion(t, p)
}
