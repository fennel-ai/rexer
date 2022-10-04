package json

import (
	"encoding/json"
	userL "fennel/mothership/lib/user"
)

func User2J(user userL.User) string {
	bytes, _ := json.Marshal(map[string]interface{}{
		"email":         user.Email,
		"firstName":     user.FirstName,
		"lastName":      user.LastName,
		"onboardStatus": user.OnboardStatus,
	})
	return string(bytes)
}
