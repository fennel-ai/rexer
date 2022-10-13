package main

import (
	"encoding/json"
	tierL "fennel/mothership/lib/tier"

	"github.com/samber/lo"
	"gorm.io/gorm"
)

func customerTiers(db *gorm.DB, customerID uint) string {
	var tiers []tierL.Tier

	if db.Where("customer_id = ?", customerID).Find(&tiers).Error != nil {
		return "[]"
	}
	var data = lo.Map(tiers, func(tier tierL.Tier, _ int) map[string]any {
		return map[string]interface{}{
			"id": tier.IDStr(),
		}
	})
	bytes, _ := json.Marshal(data)
	return string(bytes)
}
