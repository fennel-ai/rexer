package metric

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

func QueryRange(ctx context.Context, address string, query string, start, end time.Time, step time.Duration) (model.Value, error) {

	client, err := api.NewClient(api.Config{
		Address: address, //"http://a535b3af4b7e7400bab17167a1f5f7a4-766178462.ap-south-1.elb.amazonaws.com/",
	})
	if err != nil {
		return nil, err
	}

	v1api := v1.NewAPI(client)
	r := v1.Range{
		Start: start,
		End:   end,
		Step:  step,
	}
	result, warnings, err := v1api.QueryRange(ctx, query, r)
	if err != nil {
		return nil, err
	}
	if len(warnings) > 0 {
		fmt.Printf("Warnings: [metric] %v\n", warnings)
	}
	return result, err
}
