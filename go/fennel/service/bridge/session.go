package main

import (
	libuser "fennel/lib/user"
	"log"

	"github.com/gin-contrib/sessions"
)

const (
	FlashMessageTypeKey    = "flash_message_type"
	FlashMessageContentKey = "flash_message_content"
	RememberTokenKey       = "remember_token"

	FlashTypeError   = "error"
	FlashTypeSuccess = "success"
)

func addFlashMessage(session sessions.Session, msgType, msgContent string) {
	session.Set(FlashMessageTypeKey, msgType)
	session.Set(FlashMessageContentKey, msgContent)

	if err := session.Save(); err != nil {
		log.Printf("Error saving cookie: %v", err)
	}
}

func saveUserIntoCookie(session sessions.Session, user libuser.User) {
	session.Set(RememberTokenKey, user.RememberToken.String)
	if err := session.Save(); err != nil {
		log.Printf("Error saving cookie: %v", err)
	}
}
