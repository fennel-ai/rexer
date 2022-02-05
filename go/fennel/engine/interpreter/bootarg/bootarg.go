package bootarg

import (
	"fennel/tier"
	"fmt"
)

func Create(instance tier.Tier) map[string]interface{} {
	return map[string]interface{}{
		"__instance__": instance,
	}
}

func GetInstance(bootargs map[string]interface{}) (tier.Tier, error) {
	v, ok := bootargs["__instance__"]
	if !ok {
		return tier.Tier{}, fmt.Errorf("instance not found in bootargs")
	}
	ret, ok := v.(tier.Tier)
	if !ok {
		return tier.Tier{}, fmt.Errorf("__instance__ magic property had: '%v', not an instance", v)
	}
	return ret, nil
}
