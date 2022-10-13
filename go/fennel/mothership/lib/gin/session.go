package gin

import (
	userL "fennel/mothership/lib/user"
	"log"

	"github.com/gin-contrib/sessions"
)

const (
	RememberTokenKey       = "remember_token"
	FlashMessageTypeKey    = "flash_message_type"
	FlashMessageContentKey = "flash_message_content"

	FlashTypeError   = "error"
	FlashTypeSuccess = "success"
)

func SaveUserIntoCookie(session sessions.Session, user userL.User) {
	if !user.RememberToken.Valid {
		log.Printf("Error saving cookie: remember token missing")
		return
	}
	session.Set(RememberTokenKey, user.RememberToken.String)
	if err := session.Save(); err != nil {
		log.Printf("Error saving cookie: %v", err)
	}
}

func AddFlashMessage(session sessions.Session, msgType, msgContent string) {
	session.Set(FlashMessageTypeKey, msgType)
	session.Set(FlashMessageContentKey, msgContent)

	if err := session.Save(); err != nil {
		log.Printf("Error saving cookie: %v", err)
	}
}
