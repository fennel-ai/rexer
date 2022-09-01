package onboard

import (
	"context"
	"database/sql"
	"fmt"
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

type ErrorUnexpectedOnboardStatus struct {
	Status uint
	Action string
}

func (err *ErrorUnexpectedOnboardStatus) Error() string {
	return fmt.Sprintf("Unexpected onboard status %v for action %s", err.Status, err.Action)
}

func CreateTeam(ctx context.Context, db *gorm.DB, name, domain string, allowAutoJoin bool, user userL.User) (customer.Customer, uint, error) {
	var customer customer.Customer

	if user.OnboardStatus != userL.OnboardStatusSetupTeam {
		return customer, 0, &ErrorUnexpectedOnboardStatus{Status: user.OnboardStatus, Action: "CreateTeam"}
	}

	customer.Name = name
	if allowAutoJoin {
		customer.Domain = sql.NullString{String: domain, Valid: allowAutoJoin}
	} else {
		customer.Domain = sql.NullString{Valid: false}
	}
	if err := db.Create(&customer).Error; err != nil {
		return customer, 0, err
	}
	err := db.Model(&user).Updates(map[string]interface{}{
		"onboard_status": userL.OnBoardStatusAboutYourself,
		"customer_id":    customer.ID,
	}).Error
	return customer, userL.OnBoardStatusAboutYourself, err
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
