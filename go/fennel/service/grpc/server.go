package main

import (
	"context"
	"strings"

	"fennel/lib/featurestore/stream"

	streamCon "fennel/controller/featurestore/stream"
	"fennel/featurestore/tier"
	aggProto "fennel/lib/featurestore/aggregate/proto"
	featureProto "fennel/lib/featurestore/feature/proto"
	grpc "fennel/lib/featurestore/grpc/proto"
	status "fennel/lib/featurestore/status/proto"
	streamProto "fennel/lib/featurestore/stream/proto"
)

type featureStoreServer struct {
	grpc.UnimplementedFennelFeatureStoreServer
	tier tier.Tier
}

func (s *featureStoreServer) RegisterStream(ctx context.Context, req *streamProto.CreateStreamRequest) (*status.Status, error) {
	strm, err := stream.FromRequest(req)
	if err != nil {
		return &status.Status{
			Code:    2,
			Message: err.Error(),
		}, err
	}

	err = streamCon.StoreStream(ctx, s.tier, strm)
	if err != nil {
		return &status.Status{
			Code:    2,
			Message: err.Error(),
		}, err
	}

	last, failed := 0, false
	for i, src := range strm.Sources {
		err = streamCon.StoreSource(ctx, s.tier, src)
		if err != nil {
			failed = true
			last = i
			break
		}
	}
	if failed {
		// Rollback if failed to store any source
		var sourcesNotDeleted []string
		for i := 0; i < last; i++ {
			err2 := streamCon.DeleteSource(ctx, s.tier, strm.Sources[i].GetSourceName())
			if err2 != nil {
				sourcesNotDeleted = append(sourcesNotDeleted, strm.Sources[i].GetSourceName())
			}
		}
		msg := "failed to delete the following sources during rollback: " + strings.Join(sourcesNotDeleted, ",")

		err2 := streamCon.DeleteStream(ctx, s.tier, strm.Name)
		if err2 != nil {
			msg = msg + "; and failed to delete the stream"
		}

		return &status.Status{
			Code:    2,
			Message: err.Error() + "; " + msg,
		}, err
	}

	last, failed = 0, false
	for i, conn := range strm.Connectors {
		err = streamCon.StoreConnector(ctx, s.tier, conn)
		if err != nil {
			failed = true
			last = i
			break
		}
	}
	if failed {
		// Rollback if failed to store any connector
		var sourcesNotDeleted, connsNotDeleted []string
		for _, src := range strm.Sources {
			err2 := streamCon.DeleteSource(ctx, s.tier, src.GetSourceName())
			if err2 != nil {
				sourcesNotDeleted = append(sourcesNotDeleted, src.GetSourceName())
			}
		}
		for i := 0; i < last; i++ {
			err2 := streamCon.DeleteConnector(ctx, s.tier, strm.Connectors[i].Name)
			if err2 != nil {
				connsNotDeleted = append(connsNotDeleted, strm.Connectors[i].Name)
			}
		}
		msg := "failed to delete the following sources and connectors during rollback: " +
			"sources{" + strings.Join(sourcesNotDeleted, ",") + "}, " +
			"connectors{" + strings.Join(connsNotDeleted, ",") + "}"

		err2 := streamCon.DeleteStream(ctx, s.tier, strm.Name)
		if err2 != nil {
			msg = msg + "; and failed to delete the stream"
		}

		return &status.Status{
			Code:    2,
			Message: err.Error() + "; " + msg,
		}, err
	}

	return &status.Status{
		Code: 0,
	}, nil
}

func (s *featureStoreServer) RegisterAggregate(ctx context.Context, req *aggProto.CreateAggregateRequest) (*status.Status, error) {
	return &status.Status{
		Code: 0,
	}, nil
}

func (s *featureStoreServer) RegisterFeature(ctx context.Context, req *featureProto.CreateFeatureRequest) (*status.Status, error) {
	return &status.Status{
		Code: 0,
	}, nil
}
