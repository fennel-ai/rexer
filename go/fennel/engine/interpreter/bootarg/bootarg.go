package bootarg

import (
	"fmt"

	"fennel/tier"
)

func Create(tier tier.Tier) map[string]interface{} {
	return map[string]interface{}{
		"__tier__": tier,
	}
}

func GetTier(bootargs map[string]interface{}) (tier.Tier, error) {
	v, ok := bootargs["__tier__"]
	if !ok {
		return tier.Tier{}, fmt.Errorf("tier not found in bootargs")
	}
	ret, ok := v.(tier.Tier)
	if !ok {
		return tier.Tier{}, fmt.Errorf("__tier__ magic property had: '%v', not a tier", v)
	}
	return ret, nil
}
