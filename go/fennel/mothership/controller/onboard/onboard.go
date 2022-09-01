package onboard

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"fennel/mothership/lib/customer"
	tierL "fennel/mothership/lib/tier"
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

func CreateTeam(ctx context.Context, db *gorm.DB, name string, allowAutoJoin bool, user userL.User) (customer.Customer, uint, error) {
	var customer customer.Customer

	if user.OnboardStatus != userL.OnboardStatusSetupTeam {
		return customer, 0, &ErrorUnexpectedOnboardStatus{Status: user.OnboardStatus, Action: "CreateTeam"}
	}
	if user.CustomerID != 0 {
		return customer, 0, fmt.Errorf("user already has a team %v", user.Email)
	}

	customer.Name = name
	if allowAutoJoin {
		domain := extractEmailDomain(user.Email)
		if isPersonalDomain(domain) {
			return customer, 0, fmt.Errorf("personal domain %s not allowed to auto join", user.Email)
		}

		customer.Domain = sql.NullString{String: domain, Valid: true}
	} else {
		customer.Domain = sql.NullString{Valid: false}
	}
	if err := db.Create(&customer).Error; err != nil {
		return customer, 0, err
	}
	err := db.Model(&user).Updates(map[string]interface{}{
		"onboard_status": userL.OnboardStatusAboutYourself,
		"customer_id":    customer.ID,
	}).Error
	return customer, userL.OnboardStatusAboutYourself, err
}

func JoinTeam(ctx context.Context, db *gorm.DB, teamID uint, user userL.User) (uint, error) {
	if user.OnboardStatus != userL.OnboardStatusSetupTeam {
		return 0, &ErrorUnexpectedOnboardStatus{Status: user.OnboardStatus, Action: "JoinTeam"}
	}

	var customer customer.Customer
	if db.Take(&customer, teamID).Error != nil {
		return 0, fmt.Errorf("team (%v) not found", teamID)
	}
	if !customer.Domain.Valid || extractEmailDomain(user.Email) != customer.Domain.String {
		return 0, fmt.Errorf("join team (%v) not allowed for email (%s)", teamID, user.Email)
	}

	err := db.Model(&user).Update("customer_id", teamID).Error
	return userL.OnboardStatusAboutYourself, err
}

func AssignTier(ctx context.Context, db *gorm.DB, user *userL.User) (tierL.Tier, bool, error) {
	var tier tierL.Tier

	if user.CustomerID == 0 {
		return tier, false, errors.New("user doesn't have a team")
	}
	if user.OnboardStatus != userL.OnboardStatusTierProvision {
		return tier, false, &ErrorUnexpectedOnboardStatus{Status: user.OnboardStatus, Action: "AssignTier"}
	}

	// already has a tier
	if db.Where("customer_id = ?", user.CustomerID).Take(&tier).RowsAffected > 0 {
		err := db.Model(&user).Update("onboard_status", userL.OnboardStatusTierProvisioned).Error
		return tier, true, err
	}

	// TODO(xiao) potential race?
	if db.Model(&tier).Where("customer_id = ?", 0).Update("customer_id", user.CustomerID).Limit(1).RowsAffected > 0 {
		if db.Where("customer_id = ?", user.CustomerID).Take(&tier).RowsAffected > 0 {
			err := db.Model(&user).Update("onboard_status", userL.OnboardStatusTierProvisioned).Error
			return tier, true, err
		}
	}
	err := db.Model(&user).Update("onboard_status", userL.OnboardStatusTierNotAvailable).Error
	return tier, false, err
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
