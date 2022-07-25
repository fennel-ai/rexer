package sql

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterNameComparison(t *testing.T) {
	fName := filterName("test")
	assert.True(t, fName.Equal(filterName("test")))
	assert.False(t, fName.Equal(filterName("test1")))
}

func TestFilterOperatorComparisons(t *testing.T) {
	assert.True(t, EQUAL.Equal(EQUAL))
	assert.False(t, EQUAL.Equal(NOT_EQUAL))
}

func TestFilterValueDecoding(t *testing.T) {
	fVStr := "\"testvalue\""
	var f filterValue
	assert.NoError(t, json.Unmarshal([]byte(fVStr), &f))
	fVStr = "[\"testvalue1\", \"testvalue2\"]"
	assert.NoError(t, json.Unmarshal([]byte(fVStr), &f))
}

func TestFilterValueComparisons(t *testing.T) {
	fVStr1 := "\"testvalue\""
	var f1 filterValue
	assert.NoError(t, json.Unmarshal([]byte(fVStr1), &f1))
	fVStr2 := "\"testvalue\""
	var f2 filterValue
	assert.NoError(t, json.Unmarshal([]byte(fVStr2), &f2))
	assert.True(t, f1.Equal(&f2))
}

func TestSimpleSqlFilterComparisions(t *testing.T) {
	{
		str := `{
		"Name": "a",
		"Op": "=",
		"Value" : "b"
	}
	`
		filter, err := FromJSON([]byte(str))
		simpleFilter := filter.(*simpleSqlFilter)
		assert.True(t, simpleFilter.Name.Equal(filterName("a")))
		assert.True(t, simpleFilter.Value.Equal(&filterValue{
			SingleValue: "b",
		}))
		assert.True(t, simpleFilter.Op.Equal(EQUAL))
		assert.NoError(t, err)
		str2 := `{
		"Name": "a",
		"Op": "=",
		"Value" : "b"
	}
	`
		filter2, err := FromJSON([]byte(str2))
		assert.True(t, filter.Equal(filter2))
		assert.NoError(t, err)
	}

	{
		// MultiValue with ordering change.
		str := `{
		"Name": "a",
		"Op": "in",
		"Value" : ["b", "a"]
	}
	`
		filter, err := FromJSON([]byte(str))
		assert.NoError(t, err)
		str2 := `{
		"Name": "a",
		"Op": "in",
		"Value" : ["a", "b"]
	}
	`
		filter2, err := FromJSON([]byte(str2))
		assert.True(t, filter.Equal(filter2))
		assert.NoError(t, err)

	}
}

func TestCompositeFilterDecoding(t *testing.T) {
	str := `
	{
		"Left": {
			"Name": "a",
			"Op": "=",
			"Value": "b"
		},
		"Right": {
			"Name": "a",
			"Op": "=",
			"Value": "b"
		},
		"Op": "and"
	}
	`
	filter, err := FromJSON([]byte(str))
	assert.NoError(t, err)
	cFilter := filter.(*compositeSqlFilter)
	// Compare Left values.
	left := cFilter.Left.(*simpleSqlFilter)
	assert.True(t, left.Name.Equal(filterName("a")))
	assert.True(t, left.Value.Equal(&filterValue{
		SingleValue: "b",
	}))
	assert.True(t, left.Op.Equal(EQUAL))
	// Compare Left and right filter, they are the same.
	fmt.Println(cFilter.Left.String(), cFilter.Right.String())
	assert.True(t, cFilter.Left.Equal(cFilter.Right))

}

func TestCompositeFilterComparisons(t *testing.T) {
	{
		str1 := `
	{
		"Left": {
			"Name": "a",
			"Op": "=",
			"Value": "b"
		},
		"Right": {
			"Name": "c",
			"Op": "=",
			"Value": "d"
		},
		"Op": "and"
	}
	`
		filter1, err := FromJSON([]byte(str1))
		assert.NoError(t, err)
		str2 := `
	{
		"Left": {
			"Name": "a",
			"Op": "=",
			"Value": "b"
		},
		"Right": {
			"Name": "c",
			"Op": "=",
			"Value": "d"
		},
		"Op": "and"
	}
	`
		filter2, err := FromJSON([]byte(str2))
		assert.NoError(t, err)
		assert.True(t, filter1.Equal(filter2))

	}
	{
		// Test with operator tree are mirror images.
		str1 := `
	{
		"Left": {
			"Name": "a",
			"Op": "=",
			"Value": "b"
		},
		"Right": {
			"Name": "c",
			"Op": "=",
			"Value": "d"
		},
		"Op": "and"
	}
	`
		filter1, err := FromJSON([]byte(str1))
		assert.NoError(t, err)
		str2 := `
	{
		"Left": {
			"Name": "c",
			"Op": "=",
			"Value": "d"
		},
		"Right": {
			"Name": "a",
			"Op": "=",
			"Value": "b"
		},
		"Op": "and"
	}
	`
		filter2, err := FromJSON([]byte(str2))
		assert.NoError(t, err)
		assert.True(t, filter1.Equal(filter2))

	}
	{
		// Test with operator tree are mirror images as well multi value operators.
		str1 := `
	{
		"Left": {
			"Name": "a",
			"Op": "=",
			"Value": "b"
		},
		"Right": {
			"Name": "c",
			"Op": "=",
			"Value": ["d", "e"]
		},
		"Op": "and"
	}
	`
		filter1, err := FromJSON([]byte(str1))
		assert.NoError(t, err)
		str2 := `
	{
		"Left": {
			"Name": "c",
			"Op": "=",
			"Value": ["e", "d"]
		},
		"Right": {
			"Name": "a",
			"Op": "=",
			"Value": "b"
		},
		"Op": "and"
	}
	`
		filter2, err := FromJSON([]byte(str2))
		assert.NoError(t, err)
		assert.True(t, filter1.Equal(filter2))

	}

}
