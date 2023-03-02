package aggregate

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/engine/ast"
	"fennel/lib/value"
)

func TestAggregateJSON(t *testing.T) {
	type test struct {
		str string
		agg Aggregate
	}
	var tests []test
	aggs := []Aggregate{
		{Name: "some name", Timestamp: 123,
			Options: Options{
				AggType:   "some type",
				Durations: []uint32{120, 12 * 3600},
				Window:    1,
				Limit:     10},
		},
		{Timestamp: math.MaxUint32,
			Options: Options{
				Durations: []uint32{math.MaxUint32},
				Limit:     math.MaxUint32,
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
		assert.True(t, tst.agg.Equals(agg))
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
		str:  `{"Name":"","Key":null,"Kwargs":{}}`,
		gavr: GetAggValueRequest{Key: value.Nil},
	}, {
		str:  `{"Name":"some name","Key":-5,"Kwargs":{"duration":1}}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.Int(-5), Kwargs: value.NewDict(map[string]value.Value{"duration": value.Int(1)})},
	}, {
		str:  `{"Name":"some name","Key":true,"Kwargs":{"something":{}}}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.Bool(true), Kwargs: value.NewDict(map[string]value.Value{"something": value.NewDict(nil)})},
	}, {
		str:  `{"Name":"some name","Key":-12.9,"Kwargs":{}}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.Double(-12.9), Kwargs: value.NewDict(nil)},
	}, {
		str:  `{"Name":"some name","Key":"pqrs","Kwargs":{}}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.String("pqrs")},
	}, {
		str:  `{"Name":"some name","Key":[],"Kwargs":{}}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.NewList()},
	}, {
		str:  `{"Name":"some name","Key":[null],"Kwargs":{}}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.NewList(value.Nil)},
	}, {
		str:  `{"Name":"some name","Key":{},"Kwargs":{}}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.NewDict(nil)},
	}, {
		str:  `{"Name":"some name","Key":{"k1":4.5},"Kwargs":{}}`,
		gavr: GetAggValueRequest{AggName: "some name", Key: value.NewDict(map[string]value.Value{"k1": value.Double(4.5)})},
	}}
	// Test unmarshal
	for _, tst := range tests {
		var gavr GetAggValueRequest
		err := json.Unmarshal([]byte(tst.str), &gavr)
		assert.NoError(t, err)
		assert.Equal(t, tst.gavr.AggName, gavr.AggName)
		assert.True(t, tst.gavr.Key.Equal(gavr.Key))
		assert.True(t, tst.gavr.Kwargs.Equal(gavr.Kwargs))
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
	var dStr []string
	for _, d := range agg.Options.Durations {
		dStr = append(dStr, strconv.FormatUint(uint64(d), 10))
	}
	return fmt.Sprintf(
			`{"Name":"%s","Query":"%s","Timestamp":%d, `+
				`"Options":{"Type":"%s","Durations":%s,"Window":%d,"Limit":%d}}`,
			agg.Name, queryStr, agg.Timestamp,
			agg.Options.AggType, "["+strings.Join(dStr, ",")+"]",
			agg.Options.Window, agg.Options.Limit),
		nil
}
