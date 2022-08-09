package profile

import (
	"context"
	"fennel/client"
	"fennel/lib/sql"
	"fennel/mothership"
	"net/http"

	lib "fennel/lib/profile"
)

func Profiles(c context.Context, m mothership.Mothership, otype, oid string, pagination sql.Pagination) ([]lib.ProfileItem, error) {
	// TODO(xiao) client address
	cli, err := client.NewClient("http://localhost:2425", http.DefaultClient)
	if err != nil {
		return nil, err
	}
	return cli.QueryProfiles(otype, oid, pagination)
}
