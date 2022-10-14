package serializer

import (
	"encoding/json"

	"github.com/samber/lo"
	"gorm.io/gorm"

	customerL "fennel/mothership/lib/customer"
	tierL "fennel/mothership/lib/tier"
	userL "fennel/mothership/lib/user"
)

func TeamMembers2M(db *gorm.DB, customer customerL.Customer) map[string]any {
	var users []userL.User

	if db.Where("customer_id = ?", customer.ID).Find(&users).Error != nil {
		return map[string]any{
			"users": []map[string]any{},
		}
	}

	return map[string]any{
		"id":   customer.ID,
		"name": customer.Name,
		"users": lo.Map(users, func(user userL.User, _ int) map[string]any {
			return map[string]any{
				"email":     user.Email,
				"firstName": user.FirstName,
				"lastName":  user.LastName,
			}
		}),
	}
}

func CustomerTiers2J(db *gorm.DB, customerID uint) string {
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
