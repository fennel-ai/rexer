package profile

import (
	"context"
	"fennel/client"
	"fennel/lib/sql"
	"net/http"

	profileL "fennel/lib/profile"
	tierL "fennel/mothership/lib/tier"
)

func Profiles(c context.Context, tier tierL.Tier, otype, oid string, pagination sql.Pagination) ([]profileL.ProfileItem, error) {
	cli, err := client.NewClient(tier.ApiUrl, http.DefaultClient)
	if err != nil {
		return nil, err
	}
	return cli.QueryProfiles(otype, oid, pagination)
}
