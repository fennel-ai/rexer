package main

import (
	tierL "fennel/mothership/lib/tier"
	userL "fennel/mothership/lib/user"
	"net/http"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	CurrentUserKey  = "current_user"
	CurrentTierKey  = "current_tier"
	FlashMessageKey = "flash_message"
)

func AuthenticationRequired(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		session := sessions.Default(c)
		token, ok := session.Get(RememberTokenKey).(string)
		if ok && token != "" {
			var user userL.User
			result := db.Take(&user, "remember_token = ?", token)
			if result.Error == nil {
				c.Set(CurrentUserKey, user)
				return
			}
		}
		c.Redirect(http.StatusFound, SignInURL)
		c.Abort()
	}
}

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

func Onboarded(c *gin.Context) {
	user, _ := CurrentUser(c)
	if user.IsOnboarding() {
		c.HTML(http.StatusOK, "bridge/index.tmpl", gin.H{
			"title": title("Onboard"),
			"page":  OnboardPage,
			"user":  userMap(user),
		})
		c.Abort()
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

/**
 * Flash message is an one-time message stored in the user session.
 * The middleware will read the message, remove it from the session and save it in the context.
 */
func WithFlashMessage(c *gin.Context) {
	session := sessions.Default(c)
	msgType, ok := session.Get(FlashMessageTypeKey).(string)
	if !ok {
		return
	}
	msgContent, ok := session.Get(FlashMessageContentKey).(string)
	if !ok {
		return
	}
	session.Delete(FlashMessageTypeKey)
	session.Delete(FlashMessageContentKey)
	c.Set(FlashMessageKey, map[string]string{
		"flashMsgType":    msgType,
		"flashMsgContent": msgContent,
	})
}
