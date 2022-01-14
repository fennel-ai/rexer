package data

import (
	"fennel/db"
	"fennel/instance"
	"fennel/redis"
	"github.com/alicebob/miniredis/v2"
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
	err := instance.Setup([]instance.Resource{instance.DB})
	assert.NoError(t, err)
	db := DB{"profile", db.DB}
	mr, err := miniredis.Run()
	defer mr.Close()
	assert.NoError(t, err)
	client := redis.NewClient(mr.Addr(), nil)
	p := CachedDB{cache: redis.NewCache(client), groundTruth: db}
	testProviderBasic(t, p)
}

func TestCaching(t *testing.T) {
	// test that we cache the value instead of always pulling from ground truth
	mr, err := miniredis.Run()
	defer mr.Close()
	assert.NoError(t, err)
	client := redis.NewClient(mr.Addr(), nil)
	origmock := []byte{1, 2, 3}
	gt := mockProvider{origmock}
	p := CachedDB{cache: redis.NewCache(client), groundTruth: &gt}
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
	err := instance.Setup([]instance.Resource{instance.DB})
	assert.NoError(t, err)
	db := DB{"profile", db.DB}
	mr, err := miniredis.Run()
	defer mr.Close()
	assert.NoError(t, err)
	client := redis.NewClient(mr.Addr(), nil)
	p := CachedDB{cache: redis.NewCache(client), groundTruth: db}
	testProviderVersion(t, p)
}
