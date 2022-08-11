package feature

import (
	"context"
	"fennel/client"
	lib "fennel/lib/feature"
	"net/http"
)

func Features(c context.Context) ([]lib.Row, error) {
	// TODO(xiao) client address
	cli, err := client.NewClient("http://localhost:2425", http.DefaultClient)
	if err != nil {
		return nil, err
	}
	return cli.FetchRecentFeatures()
}
