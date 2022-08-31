package onboard

import (
	"context"
	"strings"

	"fennel/mothership/lib/customer"
	userL "fennel/mothership/lib/user"

	"gorm.io/gorm"
)

func TeamMatch(ctx context.Context, db *gorm.DB, user userL.User) (bool, customer.Customer, bool) {
	domain := extractEmailDomain(user.Email)
	var customer customer.Customer

	isPersonal := isPersonalDomain(domain)
	if isPersonal {
		return false, customer, isPersonal
	}

	if db.Where("domain = ?", domain).Find(&customer).RowsAffected == 0 {
		return false, customer, isPersonal
	}
	return true, customer, isPersonal
}

func isPersonalDomain(domain string) bool {
	return domain == "gmail.com" || domain == "yahoo.com" || domain == "hotmail.com" || domain == "aol.com" || domain == "hotmail.co.uk" || domain == "	hotmail.fr" || domain == "msn.com"
}

func extractEmailDomain(email string) string {
	idx := strings.Index(email, "@")
	if idx < 0 {
		return ""
	}
	return strings.ToLower(email[idx+1:])
}
