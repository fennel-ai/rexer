package stream

import (
	"context"
	"errors"
	"fmt"

	"fennel/featurestore/tier"
	lib "fennel/lib/featurestore/stream"
	"fennel/model/featurestore/stream"
)

func StoreStream(ctx context.Context, tier tier.Tier, strm lib.Stream) error {
	if err := strm.Validate(); err != nil {
		return err
	}
	// Check if stream already exists in db
	strm2, err := stream.RetrieveStream(ctx, tier, strm.Name)
	if err != nil {
		if errors.Is(err, lib.ErrStreamNotFound) {
			tier.Logger.Debug("Storing new stream " + strm.Name)
		}

		// Write stream to db
		return stream.StoreStream(ctx, tier, strm)
	}

	err = strm.Equals(strm2)
	if err != nil {
		return fmt.Errorf("stream already present but with different params")
	}
	return nil
}

func DeleteStream(ctx context.Context, tier tier.Tier, name string) error {
	// does not delete sources and connectors
	return stream.DeleteStream(ctx, tier, name)
}
