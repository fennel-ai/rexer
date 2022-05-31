package hyperparam

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestHyperParameters(t *testing.T) {
	supportedHyperParameters := HyperParamRegistry{
		"test": {
			"a": HyperParameterInfo{3, reflect.Int, []string{}},
			"b": HyperParameterInfo{4.5, reflect.Float64, []string{}},
			"c": HyperParameterInfo{"sqrt", reflect.String, []string{"none", "log", "sqrt"}},
			"d": HyperParameterInfo{"blah", reflect.String, []string{}},
		},
	}

	_, err := GetHyperParameters("test", `{"min_co_occurence": 123.5}`, supportedHyperParameters)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, doesnt support hyperparameter min_co_occurence", err.Error())

	_, err = getHyperParameters("test", `{"a": 123.5}`, supportedHyperParameters)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, hyperparameter a must be type : int", err.Error())

	h, err := getHyperParameters("test", `{"a": 123}`, supportedHyperParameters)
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"a": 123, "b": 4.5, "c": "sqrt", "d": "blah"}, h)

	_, err = getHyperParameters("test", `{"c": "fasdf"}`, supportedHyperParameters)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, hyperparameter c must be one of [none log sqrt]", err.Error())

	_, err = getHyperParameters("test", `{"d": 123.5}`, supportedHyperParameters)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, hyperparameter d must be type : string", err.Error())

	h, err = getHyperParameters("test", `{"c": "none", "d": "xyz"}`, supportedHyperParameters)
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"a": 3, "b": 4.5, "c": "none", "d": "xyz"}, h)

	_, err = getHyperParameters("test", `{"a":43, "b": "sqrt"}`, supportedHyperParameters)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, hyperparameter b must be type : float64", err.Error())

	_, err = getHyperParameters("test", `{"a":43, "b": "sqrt"}`, supportedHyperParameters)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, hyperparameter b must be type : float64", err.Error())

	h, err = getHyperParameters("test", `{"a":43, "b": 12.5}`, supportedHyperParameters)
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"a": 43, "b": 12.5, "c": "sqrt", "d": "blah"}, h)

	_, err = getHyperParameters("test", `{"a":"qrt", "b": 12.5}`, supportedHyperParameters)
	assert.Error(t, err)
	assert.Equal(t, `aggregate type: test, hyperparameter a must be type : int`, err.Error())

	_, err = getHyperParameters("test", `{"c": 12.5}`, supportedHyperParameters)
	assert.Error(t, err)
	assert.Equal(t, `aggregate type: test, hyperparameter c must be one of [none log sqrt]`, err.Error())

	_, err = getHyperParameters("test", `{"c": "sqrt", "a": "sqrt"}`, supportedHyperParameters)
	assert.Error(t, err)
	assert.Equal(t, `aggregate type: test, hyperparameter a must be type : int`, err.Error())

	h, err = getHyperParameters("test", `{"d" : "qwe", "c": "sqrt", "a": 1, "b": 2.5}`, supportedHyperParameters)
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"a": 1, "b": 2.5, "c": "sqrt", "d": "qwe"}, h)

	h, err = getHyperParameters("test", ``, supportedHyperParameters)
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"a": 3, "b": 4.5, "c": "sqrt", "d": "blah"}, h)
}
