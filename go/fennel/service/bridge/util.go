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
	bytes, _ := json.Marshal(gin.H{
		"email":         user.Email,
		"firstName":     user.FirstName,
		"lastName":      user.LastName,
		"onboardStatus": user.OnboardStatus,
	})
	return string(bytes)
}

func tierInfo(tier tierL.Tier, dp dataplaneL.DataPlane) gin.H {
	return gin.H{
		"apiUrl":   tier.ApiUrl,
		"limit":    tier.RequestsLimit,
		"location": dp.Region,
	}
}

func teamMembers(db *gorm.DB, customer customerL.Customer) gin.H {
	var users []userL.User

	if db.Where("customer_id = ?", customer.ID).Find(&users).Error != nil {
		return gin.H{
			"users": []gin.H{},
		}
	}

	return gin.H{
		"id":   customer.ID,
		"name": customer.Name,
		"users": lo.Map(users, func(user userL.User, _ int) gin.H {
			return gin.H{
				"email":     user.Email,
				"firstName": user.FirstName,
				"lastName":  user.LastName,
			}
		}),
	}
}
