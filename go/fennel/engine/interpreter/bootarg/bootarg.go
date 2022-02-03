package bootarg

import (
	"fennel/plane"
	"fmt"
)

func Create(instance plane.Plane) map[string]interface{} {
	return map[string]interface{}{
		"__instance__": instance,
	}
}

func GetInstance(bootargs map[string]interface{}) (plane.Plane, error) {
	v, ok := bootargs["__instance__"]
	if !ok {
		return plane.Plane{}, fmt.Errorf("instance not found in bootargs")
	}
	ret, ok := v.(plane.Plane)
	if !ok {
		return plane.Plane{}, fmt.Errorf("__instance__ magic property had: '%v', not an instance", v)
	}
	return ret, nil
}
