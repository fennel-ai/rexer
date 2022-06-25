package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"fennel/kafka"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/nitrous"
	"fennel/lib/value"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/resource"
	"fennel/tier"

	"google.golang.org/grpc"
)

type NitrousClient struct {
	resource.Scope

	tier   tier.Tier
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
		TierId: uint32(nc.tier.ID),
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
			TierId: uint32(nc.tier.ID),
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

func (nc NitrousClient) GetMulti(ctx context.Context, aggId ftypes.AggId, duration uint32, groupkeys []string, values []value.Value) error {
	req := &rpc.AggregateValuesRequest{
		TierId:    uint32(nc.tier.ID),
		AggId:     uint32(aggId),
		Duration:  duration,
		Groupkeys: groupkeys,
		// TODO: Make codec an argument to GetMulti instead of hard-coding.
		Codec: rpc.AggCodec_V1,
	}
	resp, err := nc.reader.GetAggregateValues(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to get aggregate values: %w", err)
	}
	for i, pv := range resp.Results {
		values[i], err = value.FromProtoValue(pv)
		if err != nil {
			return fmt.Errorf("failed to convert proto value to value: %w", err)
		}
	}
	return nil
}

func (nc NitrousClient) GetLag(ctx context.Context) (uint64, error) {
	req := &rpc.LagRequest{}
	resp, err := nc.reader.GetLag(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to get lag: %w", err)
	}
	return resp.Lag, nil
}

type NitrousClientConfig struct {
	Tier    tier.Tier
	PlaneId ftypes.RealmID
	Addr    string
}

var _ resource.Config = NitrousClientConfig{}

func (cfg NitrousClientConfig) Materialize() (resource.Resource, error) {
	scope := resource.NewPlaneScope(cfg.PlaneId)
	conn, err := grpc.Dial(cfg.Addr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nitrous: %w", err)
	}
	binlog, ok := cfg.Tier.Producers[nitrous.BINLOG_KAFKA_TOPIC]
	if !ok {
		return nil, errors.New("nitrous binlog producer not found")
	}
	return NitrousClient{
		Scope:  scope,
		tier:   cfg.Tier,
		reader: rpc.NewNitrousClient(conn),
		binlog: binlog,
	}, nil
}
