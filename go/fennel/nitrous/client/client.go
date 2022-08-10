package client

import (
	"context"
	"fmt"
	"time"

	"fennel/kafka"
	"fennel/lib/aggregate"
	"fennel/lib/arena"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/nitrous/rpc"
	"fennel/resource"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type NitrousClient struct {
	resource.Scope

	binlog kafka.FProducer
	reader rpc.NitrousClient
}

var _ resource.Resource = NitrousClient{}

func (nc NitrousClient) Close() error {
	return nil
}

func (nc NitrousClient) Type() resource.Type {
	return resource.NitrousClient
}

func (nc NitrousClient) CreateAggregate(ctx context.Context, aggId ftypes.AggId, opts aggregate.Options) error {
	popts := aggregate.ToProtoOptions(opts)
	op := &rpc.NitrousOp{
		TierId: uint32(nc.ID()),
		Type:   rpc.OpType_CREATE_AGGREGATE,
		Op: &rpc.NitrousOp_CreateAggregate{
			CreateAggregate: &rpc.CreateAggregate{
				AggId:   uint32(aggId),
				Options: popts,
			},
		},
	}
	err := nc.binlog.LogProto(ctx, op, nil)
	if err != nil {
		return fmt.Errorf("write to nitrous binlog failed: %w", err)
	}
	err = nc.binlog.Flush(5 * time.Second)
	if err != nil {
		return fmt.Errorf("failed to flush writes to nitrous binlog: %w", err)
	}
	return nil
}

func (nc NitrousClient) DeleteAggregate(ctx context.Context, aggId ftypes.AggId) error {
	op := &rpc.NitrousOp{
		TierId: uint32(nc.ID()),
		Type:   rpc.OpType_DELETE_AGGREGATE,
		Op: &rpc.NitrousOp_DeleteAggregate{
			DeleteAggregate: &rpc.DeleteAggregate{
				AggId: uint32(aggId),
			},
		},
	}
	err := nc.binlog.LogProto(ctx, op, nil)
	if err != nil {
		return fmt.Errorf("failed to forward create aggregate event to nitrous binlog: %w", err)
	}
	err = nc.binlog.Flush(5 * time.Second)
	if err != nil {
		return fmt.Errorf("failed to flush writes to nitrous binlog: %w", err)
	}
	return nil
}

// TODO: Define better error-handling semantics. Current failure handling is
// very ad-hoc - for example, we fail the entire batch if any update fails.
func (nc NitrousClient) Push(ctx context.Context, aggId ftypes.AggId, updates value.List) error {
	for i := 0; i < updates.Len(); i++ {
		update, _ := updates.At(i)
		row, ok := update.(value.Dict)
		if !ok {
			return fmt.Errorf("invalid update: %s. Expected value.Dict", update)
		}
		groupkey, ok := row.Get("groupkey")
		if !ok {
			return fmt.Errorf("update %s missing 'groupkey'", update)
		}
		vt, ok := row.Get("timestamp")
		if !ok || value.Types.Int.Validate(vt) != nil {
			return fmt.Errorf("update %s missing 'timestamp' with datatype of 'int'", update)
		}
		timestamp, _ := vt.(value.Int)
		v, ok := row.Get("value")
		if !ok {
			return fmt.Errorf("update %s missing field 'value'", update)
		}
		pv, err := value.ToProtoValue(v)
		if err != nil {
			return fmt.Errorf("failed to convert value %s to proto: %w", v, err)
		}
		op := &rpc.NitrousOp{
			TierId: uint32(nc.ID()),
			Type:   rpc.OpType_AGG_EVENT,
			Op: &rpc.NitrousOp_AggEvent{
				AggEvent: &rpc.AggEvent{
					AggId:     uint32(aggId),
					Groupkey:  groupkey.String(),
					Value:     &pv,
					Timestamp: uint32(timestamp),
				},
			},
		}
		err = nc.binlog.LogProto(ctx, op, nil)
		if err != nil {
			return fmt.Errorf("failed to log update to nitrous binlog: %w", err)
		}
	}
	err := nc.binlog.Flush(30 * time.Second)
	if err != nil {
		return fmt.Errorf("failed to flush writes to nitrous binlog: %w", err)
	}
	return nil
}

func (nc NitrousClient) GetMulti(ctx context.Context, aggId ftypes.AggId, groupkeys []value.Value, kwargs []value.Dict, output []value.Value) error {
	if len(groupkeys) != len(kwargs) {
		return fmt.Errorf("groupkeys and kwargs must be the same length %d != %d", len(groupkeys), len(kwargs))
	}
	pkwargs := make([]*value.PVDict, len(kwargs))
	strkeys := arena.Strings.Alloc(len(groupkeys), len(groupkeys))
	defer arena.Strings.Free(strkeys)
	for i := 0; i < len(kwargs); i++ {
		pk, err := value.ToProtoDict(kwargs[i])
		if err != nil {
			return fmt.Errorf("could not convert kwargs %s to proto: %w", kwargs[i], err)
		}
		pkwargs[i] = &pk
		strkeys[i] = groupkeys[i].String()
	}
	req := &rpc.AggregateValuesRequest{
		TierId:    uint32(nc.ID()),
		AggId:     uint32(aggId),
		Kwargs:    pkwargs,
		Groupkeys: strkeys,
		// TODO: Make codec an argument to GetMulti instead of hard-coding.
		Codec: rpc.AggCodec_V2,
	}
	resp, err := nc.reader.GetAggregateValues(ctx, req)
	if err != nil {
		return fmt.Errorf("rpc: %w", err)
	}
	for i, pv := range resp.Results {
		output[i], err = value.FromProtoValue(pv)
		if err != nil {
			return fmt.Errorf("could not convert proto value to value: %w", err)
		}
	}
	return nil
}

func (nc NitrousClient) GetLag(ctx context.Context) (uint64, error) {
	req := &rpc.LagRequest{}
	resp, err := nc.reader.GetLag(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("rpc: %w", err)
	}
	return resp.Lag, nil
}

type NitrousClientConfig struct {
	TierID         ftypes.RealmID
	ServerAddr     string
	BinlogProducer kafka.FProducer
}

var _ resource.Config = NitrousClientConfig{}

func (cfg NitrousClientConfig) Materialize() (resource.Resource, error) {
	conn, err := grpc.Dial(cfg.ServerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// TODO: Uncomment the following to enable distributed traces.
		// grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		// grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()),
	)
	if err != nil {
		return nil, fmt.Errorf("could not connect to nitrous: %w", err)
	}
	rpcclient := rpc.NewNitrousClient(conn)
	return NitrousClient{
		Scope:  resource.NewPlaneScope(cfg.TierID),
		reader: rpcclient,
		binlog: cfg.BinlogProducer,
	}, nil
}
