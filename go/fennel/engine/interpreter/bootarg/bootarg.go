package bootarg

import (
	"fennel/instance"
	"fmt"
)

func Create(instance instance.Instance) map[string]interface{} {
	return map[string]interface{}{
		"__instance__": instance,
	}
}

func GetInstance(bootargs map[string]interface{}) (instance.Instance, error) {
	v, ok := bootargs["__instance__"]
	if !ok {
		return instance.Instance{}, fmt.Errorf("instance not found in bootargs")
	}
	ret, ok := v.(instance.Instance)
	if !ok {
		return instance.Instance{}, fmt.Errorf("__instance__ magic property had: '%v', not an instance", v)
	}
	return ret, nil
}
