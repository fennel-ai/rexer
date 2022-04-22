//go:build !badger

package aggregate_delta

import (
	"context"
	"fennel/kafka"
	"fennel/tier"
)

func TransferAggrDeltasToDB(ctx context.Context, tr tier.Tier, consumer kafka.FConsumer) error {
	// no op
	return nil
}
