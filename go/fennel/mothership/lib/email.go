package lib

import "strings"

type StandardEmailTemplate struct {
	MothershipEndpoint string
	Subject            string
	Title              string
	Year               int

	Desc    string // optional
	CTAText string // optional
	CTALink string // optional
}

func IsPersonalDomain(domain string) bool {
	return domain == "gmail.com" || domain == "yahoo.com" || domain == "hotmail.com" || domain == "aol.com" || domain == "hotmail.co.uk" || domain == "	hotmail.fr" || domain == "msn.com"
}

func ExtractEmailDomain(email string) string {
	idx := strings.Index(email, "@")
	if idx < 0 {
		return ""
	}
	return strings.ToLower(email[idx+1:])
}

var DomainWhitelist = []string{"fennel.ai", "getlokalapp.com", "convoynetwork.com"}

func IsEmailDomainWhitelisted(email string) bool {
	domain := ExtractEmailDomain(email)
	for _, d := range DomainWhitelist {
		if d == domain {
			return true
		}
	}
	return false
}
