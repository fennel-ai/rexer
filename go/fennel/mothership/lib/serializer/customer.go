package serializer

import (
	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
	"gorm.io/gorm"

	customerL "fennel/mothership/lib/customer"
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
		"users": lo.Map(users, func(user userL.User, _ int) gin.H {
			return map[string]any{
				"email":     user.Email,
				"firstName": user.FirstName,
				"lastName":  user.LastName,
			}
		}),
	}
}
