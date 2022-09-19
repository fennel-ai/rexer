package main

import (
	"encoding/json"
	customerL "fennel/mothership/lib/customer"
	dataplaneL "fennel/mothership/lib/dataplane"
	tierL "fennel/mothership/lib/tier"
	userL "fennel/mothership/lib/user"

	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
	"gorm.io/gorm"
)

func userMap(user userL.User) string {
	// TODO(xiao) maybe add json tags on the user model
	bytes, _ := json.Marshal(map[string]interface{}{
		"email":         user.Email,
		"firstName":     user.FirstName,
		"lastName":      user.LastName,
		"onboardStatus": user.OnboardStatus,
	})
	return string(bytes)
}

func customerTiers(db *gorm.DB, customerID uint) string {
	var tiers []tierL.Tier

	if db.Where("customer_id = ?", customerID).Find(&tiers).Error != nil {
		return "[]"
	}
	var data = lo.Map(tiers, func(tier tierL.Tier, _ int) gin.H {
		return map[string]interface{}{
			"id": tier.IDStr(),
		}
	})
	bytes, _ := json.Marshal(data)
	return string(bytes)
}

func tierInfo(tier tierL.Tier, dp dataplaneL.DataPlane) map[string]any {
	return map[string]any{
		"apiUrl":   tier.ApiUrl,
		"limit":    tier.RequestsLimit,
		"location": dp.Region,
		"plan":     tier.PlanName(),
		"id":       tier.IDStr(),
	}
}

func teamMembers(db *gorm.DB, customer customerL.Customer) map[string]any {
	var users []userL.User

	if db.Where("customer_id = ?", customer.ID).Find(&users).Error != nil {
		return gin.H{
			"users": []map[string]any{},
		}
	}

	return gin.H{
		"id":   customer.ID,
		"name": customer.Name,
		"users": lo.Map(users, func(user userL.User, _ int) gin.H {
			return map[string]any{
				"email":     user.Email,
				"firstName": user.FirstName,
				"lastName":  user.LastName,
			}
		}),
	}
}
