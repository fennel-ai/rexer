package main

import (
	lib "fennel/mothership/lib/user"
	"net/http"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const (
	CurrentUserKey  = "current_user"
	FlashMessageKey = "flash_message"
)

func AuthenticationRequired(db *gorm.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		session := sessions.Default(c)
		token, ok := session.Get(RememberTokenKey).(string)
		if ok && token != "" {
			var user lib.User
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

func CurrentUser(c *gin.Context) (lib.User, bool) {
	userAny, ok := c.Get(CurrentUserKey)
	if !ok {
		return lib.User{}, false
	}
	user, ok := userAny.(lib.User)
	return user, ok
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
