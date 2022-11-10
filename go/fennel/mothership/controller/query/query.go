package feature

import (
	"context"
	"fennel/client"
	queryL "fennel/lib/query"
	tierL "fennel/mothership/lib/tier"
	"net/http"
)

func ListQueries(c context.Context, tier tierL.Tier) ([]queryL.QuerySer, error) {
	cli, err := client.NewClient(tier.ApiUrl, http.DefaultClient)
	if err != nil {
		return nil, err
	}
	return cli.FetchStoredQueries()
}
