package tvalue

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/value"
)

func TestTagset(t *testing.T) {
	ts1 := newTagset()
	assert.Empty(t, ts1.Tags())

	ts2 := ts1.addTag("hi", "2")
	assert.Empty(t, ts1.Tags())
	equals(t, map[string][]string{"hi": {"2"}}, ts2.Tags())

	// add a few more tags and verify set property etc.
	tsn := ts2.addTag("bye", "ok").addTag("hi", "2").addTag("hi", "last")
	equals(t, map[string][]string{"hi": {"2", "last"}, "bye": {"ok"}}, tsn.Tags())

	// now add more tags to ts1 and it should not impact ts2 at all
	ts3 := ts1.addTag("bye", "1")
	assert.Empty(t, ts1.Tags())
	equals(t, map[string][]string{"hi": {"2"}}, ts2.Tags())
	equals(t, map[string][]string{"bye": {"1"}}, ts3.Tags())
}

func TestTValue(t *testing.T) {
	tv1 := NewTValue(value.Int(1))
	assert.Empty(t, tv1.Tags())

	// can natively access all value methods on it
	found, err := tv1.Op("+", value.Int(2))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(3), found)

	// and take value to typecast
	vint, ok := tv1.Value.(value.Int)
	assert.True(t, ok)
	assert.Equal(t, value.Int(1), vint)

	// can add tags, inherit them, and print them
	v1 := "hello"
	v2 := "something"
	v3 := "else"

	tv1.Tag("hi", v1)
	equals(t, map[string][]string{"hi": {v1}}, tv1.Tags())

	tv2 := NewTValue(value.Int(121))
	tv2.Tag("hi", v2)
	equals(t, map[string][]string{"hi": {v2}}, tv2.Tags())

	tv3 := NewTValue(value.Int(221))
	tv3.Tag("hi", v3)
	equals(t, map[string][]string{"hi": {v3}}, tv3.Tags())

	tv4 := NewTValue(value.Nil, tv1)
	equals(t, tv1.Tags(), tv4.Tags())
	tv4.InheritTags(tv2, tv3)
	equals(t, map[string][]string{"hi": {v1, v2, v3}}, tv4.Tags())

	// even if tv1's tags change now, no effect on tv4 (and also test selftag along the way)
	tv1.SelfTag("bye")
	equals(t, map[string][]string{"hi": {v1}, "bye": {value.Int(1).String()}}, tv1.Tags())
	equals(t, map[string][]string{"hi": {v1, v2, v3}}, tv4.Tags())
}

func equals(t *testing.T, m1, m2 map[string][]string) {
	assert.Equal(t, len(m1), len(m2))
	for k, v1 := range m1 {
		v2, ok := m2[k]
		assert.True(t, ok)
		assert.ElementsMatch(t, v2, v1)
	}
}
