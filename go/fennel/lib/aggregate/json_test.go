package aggregate

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"fennel/engine/ast"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
)

func TestAggregateJSON(t *testing.T) {
	type test struct {
		str string
		agg Aggregate
	}
	var tests []test
	aggs := []Aggregate{
		{},
		{Name: "some name", Timestamp: 123,
			Options: Options{
				AggType:  "some type",
				Duration: 12 * 3600,
				Window:   1,
				Limit:    10},
		},
		{Timestamp: math.MaxUint64,
			Options: Options{
				Duration: math.MaxUint64,
				Limit:    math.MaxUint64,
			},
		},
	}
	for i, q := range ast.TestExamples {
		agg := aggs[i%len(aggs)]
		agg.Query = q
		s, err := makeAggregateJSON(&agg)
		assert.NoError(t, err)
		tests = append(tests, test{s, agg})
	}

	// Test unmarshal
	for _, tst := range tests {
		var agg Aggregate
		err := json.Unmarshal([]byte(tst.str), &agg)
		assert.NoError(t, err)
		assert.Equal(t, tst.agg, agg)
	}
	// Test marshal
	for _, tst := range tests {
		// Ast does not serialize to a unique string
		// So test by converting to and from JSON
		ser, err := json.Marshal(tst.agg)
		assert.NoError(t, err)
		var agg Aggregate
		err = json.Unmarshal(ser, &agg)
		assert.NoError(t, err)
		assert.Equal(t, tst.agg, agg)
	}
}

func TestGetAggValueRequestJSON(t *testing.T) {
	tests := []struct {
		str  string
		gavr GetAggValueRequest
	}{{
		str:  `{"Name":"","Key":null}`,
		gavr: GetAggValueRequest{Key: value.Nil},
	}, {
		str:  `{"Name":"some name","Key":-5}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.Int(-5)},
	}, {
		str:  `{"Name":"some name","Key":true}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.Bool(true)},
	}, {
		str:  `{"Name":"some name","Key":-12.9}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.Double(-12.9)},
	}, {
		str:  `{"Name":"some name","Key":"pqrs"}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.String("pqrs")},
	}, {
		str:  `{"Name":"some name","Key":[null]}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.List{value.Nil}},
	}, {
		str:  `{"Name":"some name","Key":{"k1":4.5}}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.Dict{"k1": value.Double(4.5)}},
	}}
	// Test unmarshal
	for _, tst := range tests {
		var gavr GetAggValueRequest
		err := json.Unmarshal([]byte(tst.str), &gavr)
		assert.NoError(t, err)
		assert.Equal(t, tst.gavr.AggName, gavr.AggName)
		assert.True(t, tst.gavr.Key.Equal(gavr.Key))
	}
	// Test marshal
	for _, tst := range tests {
		ser, err := json.Marshal(tst.gavr)
		assert.NoError(t, err)
		assert.Equal(t, tst.str, string(ser))
	}
}

func makeAggregateJSON(agg *Aggregate) (string, error) {
	querySer, err := ast.Marshal(agg.Query)
	if err != nil {
		return "", err
	}
	queryStr := base64.StdEncoding.EncodeToString(querySer)
	return fmt.Sprintf(
			`{"Name":"%s","Query":"%s","Timestamp":%d,`+
				`"Options":{"Type":"%s","Duration":%d,"Window":%d,"Limit":%d}}`,
			agg.Name, queryStr, agg.Timestamp,
			agg.Options.AggType, agg.Options.Duration, agg.Options.Window, agg.Options.Limit),
		nil
}
