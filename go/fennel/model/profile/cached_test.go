package profile

import (
	"testing"

	"fennel/lib/ftypes"
	"fennel/plane"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

type mockProvider struct {
	ret []byte
}

func (m *mockProvider) change(n []byte) {
	m.ret = n
}
func (m *mockProvider) set(this plane.Plane, custid ftypes.CustID, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	return nil
}
func (m *mockProvider) get(this plane.Plane, custid ftypes.CustID, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	return m.ret, nil
}

var _ provider = &mockProvider{}

func TestCachedDBBasic(t *testing.T) {
	testProviderBasic(t, cachedProvider{base: dbProvider{}})
}

func TestCaching(t *testing.T) {
	// test that we cache the value instead of always pulling from ground truth
	this, err := test.MockPlane()
	assert.NoError(t, err)

	origmock := []byte{1, 2, 3}
	gt := mockProvider{origmock}
	p := cachedProvider{base: &gt}
	//p := CachedDB{cache: redis.NewCache(client.(redis.Client)), groundTruth: &gt}
	//err = p.Init()
	//assert.NoError(t, err)

	// initially we should get the mocked origmock value back
	found, err := p.get(this, 1, "1", 1232, "summary", 1)
	assert.NoError(t, err)
	assert.Equal(t, origmock, found)

	// now change the mocked value
	newmock := []byte{4, 5}
	gt.change(newmock)

	// we should still get origmock back because it's in cache
	found, err = p.get(this, 1, "1", 1232, "summary", 1)
	assert.NoError(t, err)
	assert.Equal(t, origmock, found)

	// but if we set a new value, we will delete the key (remember: we don't fill cache on sets)
	err = p.set(this, 1, "1", 1232, "summary", 1, []byte{7, 8, 9})
	assert.NoError(t, err)

	// so subsequent gets should get the new updated mock back
	found, err = p.get(this, 1, "1", 1232, "summary", 1)
	assert.Equal(t, newmock, found)
}

func TestCachedDBVersion(t *testing.T) {
	testProviderVersion(t, cachedProvider{base: dbProvider{}})
}
