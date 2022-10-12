package main

import (
	tierL "fennel/mothership/lib/tier"
	userL "fennel/mothership/lib/user"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	CurrentUserKey  = "current_user"
	CurrentTierKey  = "current_tier"
	FlashMessageKey = "flash_message"
)

func TierPermission(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var tierParams struct {
			ID uint `uri:"id" binding:"required"`
		}
		if err := c.ShouldBindUri(&tierParams); err != nil {
			c.String(http.StatusBadRequest, "tier id missing in URL")
			c.Abort()
			return
		}
		var tier tierL.Tier
		if err := db.Take(&tier, tierParams.ID).Error; err != nil {
			c.String(http.StatusNotFound, "")
			c.Abort()
			return
		}

		user, ok := CurrentUser(c)
		if !ok {
			// shouldn't happen, just in case
			c.Redirect(http.StatusFound, SignInURL)
			c.Abort()
			return
		}

		if tier.CustomerID != user.CustomerID {
			c.String(http.StatusForbidden, "")
			c.Abort()
			return
		}

		c.Set(CurrentTierKey, tier)
	}
}

func CurrentUser(c *gin.Context) (userL.User, bool) {
	userAny, ok := c.Get(CurrentUserKey)
	if !ok {
		return userL.User{}, false
	}
	user, ok := userAny.(userL.User)
	return user, ok
}

func CurrentTier(c *gin.Context) (tierL.Tier, bool) {
	tierAny, ok := c.Get(CurrentTierKey)
	if !ok {
		return tierL.Tier{}, false
	}
	tier, ok := tierAny.(tierL.Tier)
	return tier, ok
}

func Onboarded(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		user, _ := CurrentUser(c)
		if user.IsOnboarding() {
			c.Redirect(http.StatusFound, "/onboard")
			c.Abort()
		}
	}
}
