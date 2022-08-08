package main

import (
	"fennel/mothership"
	"fennel/mothership/model/user"
	"net/http"

	"github.com/gin-contrib/sessions"
	"github.com/gin-gonic/gin"
)

const (
	CurrentUserKey  = "current_user"
	FlashMessageKey = "flash_message"
)

func AuthenticationRequired(mothership mothership.Mothership) gin.HandlerFunc {
	return func(c *gin.Context) {
		session := sessions.Default(c)
		token, ok := session.Get(RememberTokenKey).(string)
		if ok && token != "" {
			if user, err := user.FetchByRememberToken(mothership, token); err == nil {
				c.Set(CurrentUserKey, user)
				return
			}
		}
		c.Redirect(http.StatusFound, SignInURL)
		c.Abort()
	}
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
