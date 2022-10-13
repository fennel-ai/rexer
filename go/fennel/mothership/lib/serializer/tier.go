package serializer

import (
	dataplaneL "fennel/mothership/lib/dataplane"
	tierL "fennel/mothership/lib/tier"
)

func Tier2M(tier tierL.Tier, dp dataplaneL.DataPlane) map[string]any {
	return map[string]any{
		"apiUrl":   tier.ApiUrl,
		"limit":    tier.RequestsLimit,
		"location": dp.Region,
		"plan":     tier.PlanName(),
		"id":       tier.IDStr(),
	}
}
