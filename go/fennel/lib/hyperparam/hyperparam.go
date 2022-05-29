package hyperparam

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
)

type HyperParameterInfo struct {
	Default interface{}  `json:"default"`
	Type    reflect.Kind `json:"type"`
	Options []string     `json:"options"`
}

//================================================
// Public API for Phaser
//================================================

type HyperParamRegistry = map[string]map[string]HyperParameterInfo

func GetHyperParameters(key string, hyperparameters string, supportedHyperParameters HyperParamRegistry) (string, error) {
	return getHyperParameters(key, hyperparameters, supportedHyperParameters)
}

//================================================
// Private helpers/interface
//================================================

func getHyperParameters(key string, hyperparameters string, supportedHyperParameters HyperParamRegistry) (string, error) {
	var aggParams map[string]json.RawMessage
	if len(hyperparameters) != 0 {
		err := json.Unmarshal([]byte(hyperparameters), &aggParams)
		if err != nil {
			return "", fmt.Errorf("aggregate type: %v, failed to parse aggregate tuning params: %v", key, err)
		}
	}

	if _, ok := supportedHyperParameters[key]; !ok {
		return "", fmt.Errorf("aggregate type: %v, doesnt support hyperparameters", key)
	}
	hyperparamtersMap := supportedHyperParameters[key]

	for param, value := range aggParams {

		if _, ok := hyperparamtersMap[param]; !ok {
			return "", fmt.Errorf("aggregate type: %v, doesnt support hyperparameter %v", key, param)
		}

		if len(hyperparamtersMap[param].Options) > 0 {
			var s string
			_ = json.Unmarshal(value, &s)
			if !contains(hyperparamtersMap[param].Options, s) {
				return "", fmt.Errorf("aggregate type: %v, hyperparameter %v must be one of %v", key, param, hyperparamtersMap[param].Options)
			}
			continue
		}

		s := string(value)

		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			if hyperparamtersMap[param].Type != reflect.Int {
				return "", fmt.Errorf("aggregate type: %v, hyperparameter %v must be type : %v", key, param, hyperparamtersMap[param].Type)
			}
			continue
		}

		if _, err := strconv.ParseFloat(s, 64); err == nil {
			if hyperparamtersMap[param].Type != reflect.Float64 {
				return "", fmt.Errorf("aggregate type: %v, hyperparameter %v must be type : %v", key, param, hyperparamtersMap[param].Type)
			}
			continue
		}

		if hyperparamtersMap[param].Type == reflect.Int || hyperparamtersMap[param].Type == reflect.Float64 {
			return "", fmt.Errorf("aggregate type: %v, hyperparameter %v must be type : %v", key, param, hyperparamtersMap[param].Type)
		}
	}

	var retParams map[string]interface{}
	if len(hyperparameters) != 0 {
		_ = json.Unmarshal([]byte(hyperparameters), &retParams)
	} else {
		retParams = make(map[string]interface{})
	}

	for param := range hyperparamtersMap {
		if _, ok := retParams[param]; !ok {
			retParams[param] = hyperparamtersMap[param].Default
		}
	}

	str, err := json.Marshal(retParams)
	if err != nil {
		return "", fmt.Errorf("failed to marshal hyper params: %v", err)
	}
	return string(str), nil
}

func contains(sl []string, name string) bool {
	for _, value := range sl {
		if value == name {
			return true
		}
	}
	return false
}
