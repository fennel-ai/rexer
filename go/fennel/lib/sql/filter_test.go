package sql

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilterLeftComparison(t *testing.T) {
	fLeft := filterName("test")
	assert.True(t, fLeft.Equal(filterName("test")))
	assert.False(t, fLeft.Equal(filterName("test1")))
}

func TestFilterOperatorComparisons(t *testing.T) {
	assert.True(t, EQUAL.Equal(EQUAL))
	assert.False(t, EQUAL.Equal(NOT_EQUAL))
}

func TestFilterRightDecoding(t *testing.T) {
	fVStr := "\"testvalue\""
	var f filterValue
	assert.NoError(t, json.Unmarshal([]byte(fVStr), &f))
	fVStr = "[\"testvalue1\", \"testvalue2\"]"
	assert.NoError(t, json.Unmarshal([]byte(fVStr), &f))
}

func TestFilterRightComparisons(t *testing.T) {
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
		var filter CompositeSqlFilter
		var filter2 CompositeSqlFilter
		str := `{
		"Left": "a",
		"Op": "=",
		"Right" : "b"
	}
	`

		assert.NoError(t, json.Unmarshal([]byte(str), &filter))
		assert.True(t, filter.Left.Equal(filterName("a")))
		assert.True(t, filter.Right.Equal(&filterValue{
			SingleValue: "b",
		}))
		assert.True(t, filter.Op.Equal(EQUAL))
		str2 := `{
		"Left": "a",
		"Op": "=",
		"Right" : "b"
	}
	`
		assert.NoError(t, json.Unmarshal([]byte(str2), &filter2))
		assert.True(t, filter.Equal(&filter2))
	}

	{
		var filter CompositeSqlFilter
		var filter2 CompositeSqlFilter
		// MultiRight with ordering change.
		str := `{
		"Left": "a",
		"Op": "in",
		"Right" : ["b", "a"]
	}
	`
		assert.NoError(t, json.Unmarshal([]byte(str), &filter))
		str2 := `{
		"Left": "a",
		"Op": "in",
		"Right" : ["a", "b"]
	}
	`
		assert.NoError(t, json.Unmarshal([]byte(str2), &filter2))
		assert.True(t, filter.Equal(&filter2))

	}
}

func TestCompositeFilterDecoding(t *testing.T) {
	var filter CompositeSqlFilter
	str := `
	{
		"Left": {
			"Left": "a",
			"Op": "=",
			"Right": "b"
		},
		"Right": {
			"Left": "a",
			"Op": "=",
			"Right": "b"
		},
		"Op": "and"
	}
	`
	assert.NoError(t, json.Unmarshal([]byte(str), &filter))
	// Compare Left values.
	left := filter.Left.(*CompositeSqlFilter)
	assert.True(t, left.Left.Equal(filterName("a")))
	assert.True(t, left.Right.Equal(&filterValue{
		SingleValue: "b",
	}))
	assert.True(t, left.Op.Equal(EQUAL))
	// Compare Left and right filter, they are the same.
	fmt.Println(filter.Left.String(), filter.Right.String())
	assert.True(t, filter.Left.Equal(filter.Right))

}

func TestCompositeFilterComparisons(t *testing.T) {
	{
		var filter1 CompositeSqlFilter
		var filter2 CompositeSqlFilter
		str1 := `
	{
		"Left": {
			"Left": "a",
			"Op": "=",
			"Right": "b"
		},
		"Right": {
			"Left": "c",
			"Op": "=",
			"Right": "d"
		},
		"Op": "and"
	}
	`

		assert.NoError(t, json.Unmarshal([]byte(str1), &filter1))
		str2 := `
	{
		"Left": {
			"Left": "a",
			"Op": "=",
			"Right": "b"
		},
		"Right": {
			"Left": "c",
			"Op": "=",
			"Right": "d"
		},
		"Op": "and"
	}
	`
		assert.NoError(t, json.Unmarshal([]byte(str2), &filter2))
		assert.True(t, filter1.Equal(&filter2))
	}
	{
		var filter1 CompositeSqlFilter
		var filter2 CompositeSqlFilter
		// Test with operator tree are mirror images.
		str1 := `
	{
		"Left": {
			"Left": "a",
			"Op": "=",
			"Right": "b"
		},
		"Right": {
			"Left": "c",
			"Op": "=",
			"Right": "d"
		},
		"Op": "and"
	}
	`
		assert.NoError(t, json.Unmarshal([]byte(str1), &filter1))
		str2 := `
	{
		"Left": {
			"Left": "c",
			"Op": "=",
			"Right": "d"
		},
		"Right": {
			"Left": "a",
			"Op": "=",
			"Right": "b"
		},
		"Op": "and"
	}
	`
		assert.NoError(t, json.Unmarshal([]byte(str2), &filter2))
		assert.Equal(t, filter1.Hash(), filter2.Hash())
		assert.True(t, filter1.Equal(&filter2))
	}
	{
		// Test with operator tree are mirror images as well multi value operators.
		var filter1 CompositeSqlFilter
		var filter2 CompositeSqlFilter
		str1 := `
	{
		"Left": {
			"Left": "a",
			"Op": "=",
			"Right": "b"
		},
		"Right": {
			"Left": "c",
			"Op": "=",
			"Right": ["d", "e"]
		},
		"Op": "and"
	}
	`
		assert.NoError(t, json.Unmarshal([]byte(str1), &filter1))
		str2 := `
	{
		"Left": {
			"Left": "c",
			"Op": "=",
			"Right": ["e", "d"]
		},
		"Right": {
			"Left": "a",
			"Op": "=",
			"Right": "b"
		},
		"Op": "and"
	}
	`
		assert.NoError(t, json.Unmarshal([]byte(str2), &filter2))
		assert.True(t, filter1.Equal(&filter2))

	}
}

func TestMarshalUnmarshal(t *testing.T) {
	strs := []string{
		`{
			"Left": "a",
			"Op": "=",
			"Right": "b"
		}`,
		`{
			"Left": {
				"Left": "a",
				"Op": "=",
				"Right": "b"
			},
			"Op": "and",
			"Right": {
				"Left": "c",
				"Op": "=",
				"Right": "d"
			}
		}`,
		`
		{
			"Left": {
				"Left": "a",
				"Op": "=",
				"Right": "b"
			},
			"Op": "and",
			"Right": {
				"Left": "c",
				"Op": "=",
				"Right": "d"
			}
		}`,
	}
	for _, str := range strs {
		// Test simple marshal and unmarshal
		var filter CompositeSqlFilter
		assert.NoError(t, json.Unmarshal([]byte(str), &filter))
		b, err := json.Marshal(&filter)
		assert.NoError(t, err)
		var filter2 CompositeSqlFilter
		assert.NoError(t, json.Unmarshal(b, &filter2))
		assert.True(t, filter.Equal(&filter2))

	}
}
