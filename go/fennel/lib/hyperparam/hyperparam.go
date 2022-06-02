package hyperparam

import (
	"encoding/json"
	"fennel/lib/value"
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
// Public API
//================================================

type HyperParamRegistry = map[string]map[string]HyperParameterInfo

func GetHyperParameters(key string, hyperparameters string, supportedHyperParameters HyperParamRegistry) (map[string]interface{}, error) {
	return getHyperParameters(key, hyperparameters, supportedHyperParameters)
}

func GetHyperParametersFromMap(key string, hyperparameters value.Dict, supportedHyperParameters HyperParamRegistry) (map[string]interface{}, error) {
	return getHyperParametersFromMap(key, hyperparameters, supportedHyperParameters)
}

//================================================
// Private helpers/interface
//================================================

func getHyperParameters(key string, hyperparameters string, supportedHyperParameters HyperParamRegistry) (map[string]interface{}, error) {
	var aggParams map[string]json.RawMessage
	if len(hyperparameters) != 0 {
		err := json.Unmarshal([]byte(hyperparameters), &aggParams)
		if err != nil {
			return nil, fmt.Errorf("aggregate type: %v, failed to parse aggregate tuning params: %v", key, err)
		}
	}

	if _, ok := supportedHyperParameters[key]; !ok {
		return nil, fmt.Errorf("aggregate type: %v, doesnt support hyperparameters", key)
	}
	hyperparamtersMap := supportedHyperParameters[key]

	for param, value := range aggParams {

		if _, ok := hyperparamtersMap[param]; !ok {
			return nil, fmt.Errorf("aggregate type: %v, doesnt support hyperparameter %v", key, param)
		}

		if hyperparamtersMap[param].Options != nil && len(hyperparamtersMap[param].Options) > 0 {
			var s string
			_ = json.Unmarshal(value, &s)
			if !contains(hyperparamtersMap[param].Options, s) {
				return nil, fmt.Errorf("aggregate type: %v, hyperparameter %v must be one of %v", key, param, hyperparamtersMap[param].Options)
			}
			continue
		}
		err := validHyperParam(key, param, string(value), hyperparamtersMap[param])
		if err != nil {
			return nil, err
		}
	}

	var retParams map[string]interface{}
	if len(hyperparameters) != 0 {
		_ = json.Unmarshal([]byte(hyperparameters), &retParams)
		for param, v := range retParams {
			if hyperparamtersMap[param].Type == reflect.Int {
				retParams[param] = int(v.(float64))
			}
		}
	} else {
		retParams = make(map[string]interface{})
	}

	for param := range hyperparamtersMap {
		if _, ok := retParams[param]; !ok {
			retParams[param] = hyperparamtersMap[param].Default
		}
	}

	return retParams, nil
}

func getHyperParametersFromMap(key string, hyperparameters value.Dict, supportedHyperParameters HyperParamRegistry) (map[string]interface{}, error) {
	if _, ok := supportedHyperParameters[key]; !ok {
		return nil, fmt.Errorf("aggregate type: %v, doesnt support hyperparameters", key)
	}
	hyperparamtersMap := supportedHyperParameters[key]

	for param, value := range hyperparameters.Iter() {
		if _, ok := hyperparamtersMap[param]; !ok {
			return nil, fmt.Errorf("aggregate type: %v, doesnt support hyperparameter %v", key, param)
		}
		err := validHyperParam(key, param, value.String(), hyperparamtersMap[param])
		if err != nil {
			return nil, err
		}
	}

	var retParams map[string]interface{}
	if hyperparameters.Len() != 0 {
		for param, v := range hyperparameters.Iter() {
			if hyperparamtersMap[param].Type == reflect.Int || hyperparamtersMap[param].Type == reflect.Float64 {
				f, err := getDouble(v)
				if err != nil {
					return nil, err
				}
				if hyperparamtersMap[param].Type == reflect.Int {
					retParams[param] = int(f)
				}
			} else {
				retParams[param] = v.String()
			}
		}
	} else {
		retParams = make(map[string]interface{})
	}

	for param := range hyperparamtersMap {
		if _, ok := retParams[param]; !ok {
			retParams[param] = hyperparamtersMap[param].Default
		}
	}

	return retParams, nil
}

func validHyperParam(key, param, val string, hyperparameterInfo HyperParameterInfo) error {
	if hyperparameterInfo.Options != nil && len(hyperparameterInfo.Options) > 0 {
		if !contains(hyperparameterInfo.Options, val) {
			return fmt.Errorf("aggregate type: %v, hyperparameter %v must be one of %v", key, param, hyperparameterInfo.Options)
		}
		return nil
	}

	if _, err := strconv.ParseInt(val, 10, 64); err == nil {
		if hyperparameterInfo.Type != reflect.Int {
			return fmt.Errorf("aggregate type: %v, hyperparameter %v must be type : %v", key, param, hyperparameterInfo.Type)
		}
		return nil
	}

	if _, err := strconv.ParseFloat(val, 64); err == nil {
		if hyperparameterInfo.Type != reflect.Float64 {
			return fmt.Errorf("aggregate type: %v, hyperparameter %v must be type : %v", key, param, hyperparameterInfo.Type)
		}
		return nil
	}

	if hyperparameterInfo.Type == reflect.Int || hyperparameterInfo.Type == reflect.Float64 {
		return fmt.Errorf("aggregate type: %v, hyperparameter %v must be type : %v", key, param, hyperparameterInfo.Type)
	}

	return nil
}

func getDouble(v value.Value) (float64, error) {
	if d, ok := v.(value.Double); ok {
		return float64(d), nil
	}

	if i, ok := v.(value.Int); ok {
		return float64(i), nil
	}
	return 0, fmt.Errorf("value [%s] is not a $$ number", v.String())
}

func contains(sl []string, name string) bool {
	for _, value := range sl {
		if value == name {
			return true
		}
	}
	return false
}
