package metric

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"gorm.io/gorm"

	dataplaneL "fennel/mothership/lib/dataplane"
	tierL "fennel/mothership/lib/tier"
)

func QueryRange(ctx context.Context, db *gorm.DB, tier tierL.Tier, query string, start, end time.Time, step time.Duration) (model.Value, error) {
	var dp dataplaneL.DataPlane
	if err := db.Take(&dp, tier.DataPlaneID).Error; err != nil {
		return nil, errors.New("no data plane associated with the tier")
	}

	client, err := api.NewClient(api.Config{
		Address: dp.MetricsServerAddress,
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
