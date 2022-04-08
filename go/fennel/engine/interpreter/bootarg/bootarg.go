package bootarg

import (
	"fmt"

	"fennel/lib/profile"
	"fennel/tier"
)

func Create(tier tier.Tier) map[string]interface{} {
	return map[string]interface{}{
		"__tier__":            tier,
		"__cached_profiles__": &[]profile.ProfileItem{},
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

func GetCachedProfiles(bootargs map[string]interface{}) (*[]profile.ProfileItem, error) {
	v, ok := bootargs["__cached_profiles__"]
	if !ok {
		return nil, fmt.Errorf("cached profiles not found in bootargs")
	}
	ret, ok := v.(*[]profile.ProfileItem)
	if !ok {
		return nil, fmt.Errorf("expected __cached_profiles__ to be of type '*[]profile.ProfileItem' but found: %v", ret)
	}
	return ret, nil
}
