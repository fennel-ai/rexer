package feature

import (
	"context"
	"fennel/client"
	featureL "fennel/lib/feature"
	tierL "fennel/mothership/lib/tier"
	"net/http"
)

func Features(c context.Context, tier tierL.Tier) ([]featureL.Row, error) {
	cli, err := client.NewClient(tier.ApiUrl, http.DefaultClient)
	if err != nil {
		return nil, err
	}
	return cli.FetchRecentFeatures()
}
