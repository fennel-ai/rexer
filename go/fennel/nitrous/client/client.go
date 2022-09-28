package client

import (
	"context"
	"fmt"
	"time"

	"fennel/kafka"
	"fennel/lib/aggregate"
	"fennel/lib/arena"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/value"
	"fennel/nitrous"
	"fennel/nitrous/rpc"
	"fennel/resource"

	"github.com/samber/mo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type NitrousClient struct {
	resource.Scope

	binlog kafka.FProducer
	aggregateConf kafka.FProducer
	binlogPartitions uint32
	reader rpc.NitrousClient

	reqCh chan<- getRequest
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
	err := nc.aggregateConf.LogProto(ctx, op, nil)
	if err != nil {
		return fmt.Errorf("write to aggregate configuration failed: %w", err)
	}
	err = nc.aggregateConf.Flush(5 * time.Second)
	if err != nil {
		return fmt.Errorf("failed to flush writes to aggregate configuration: %w", err)
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
	err := nc.aggregateConf.LogProto(ctx, op, nil)
	if err != nil {
		return fmt.Errorf("failed to forward create aggregate event: %w", err)
	}
	err = nc.aggregateConf.Flush(5 * time.Second)
	if err != nil {
		return fmt.Errorf("failed to flush writes to aggregate configurations: %w", err)
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
		gk, ok := row.Get("groupkey")
		if !ok {
			return fmt.Errorf("update %s missing 'groupkey'", update)
		}
		groupkey := gk.String()
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
		op := rpc.NitrousOpFromVTPool()
		defer op.ReturnToVTPool()
		op.TierId = uint32(nc.ID())
		op.Type = rpc.OpType_AGG_EVENT
		op.Op = &rpc.NitrousOp_AggEvent{
			AggEvent: &rpc.AggEvent{
				AggId:     uint32(aggId),
				Groupkey:  groupkey,
				Value:     &pv,
				Timestamp: uint32(timestamp),
			},
		}
		// compute the hash and push to that specific partition
		//
		// we could have relied on Kafka key partitioner, but we need to perform a similar operation on the read path
		// (i.e. nitrous need to know which partition/shard would contain information of a certain groupkey)
		partition := nitrous.HashedPartition(groupkey, nc.binlogPartitions)
		d, err := op.MarshalVT()
		if err != nil {
			return fmt.Errorf("failed to marshal: %w", err)
		}
		err = nc.binlog.LogToPartition(ctx, d, int32(partition), nil)
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
	ctx, t := timer.Start(ctx, nc.ID(), "nitrous.client.GetMulti")
	defer t.Stop()
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
		return err
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
	BinlogPartitions uint32
	BinlogProducer kafka.FProducer
	ReqsLogProducer kafka.FProducer
	AggregateConfProducer kafka.FProducer
}

var _ resource.Config = NitrousClientConfig{}

type getRequest struct {
	ctx    context.Context
	msg    *rpc.AggregateValuesRequest
	respCh chan<- mo.Result[[]*value.PValue]
}

func (cfg NitrousClientConfig) Materialize() (resource.Resource, error) {
	conn, err := grpc.Dial(cfg.ServerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// TODO: Uncomment the following to enable distributed traces.
		// grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
		// grpc.WithStreamInterceptor(otelgrpc.StreamClientInterceptor()),

		// keepalive connections - these are required so that the underlying TCP connections (and hence the streams)
		// are not broken due to inactivity from the client (and also to notify any intermediate services e.g. linkerd
		// to avoid breaking a connection when there is no traffic)
		//
		// we can have query servers running for 10s of minutes even when they don't see any traffic, it is important
		// to keep these connections alive to avoid rebuilding them on new traffic
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// Duration after which the client starts pinging the server to check transport health
			Time: 10 * time.Second,
			// Timeout for the keepalive check
			Timeout: 5 * time.Second,
			// Whether these keepalives should be sent even if there are no RPCs
			PermitWithoutStream: true,
		}),

		// Configure on how the new connections are to be established in both, new connections and re-establishing
		// broken connections scenarios
		//
		// These are configured to be more aggressive than the default configurations since it is possible that the
		// connection with the nitrous server is broken (due to nitrous server restart or similar) and it is better
		// to aggressively establish connection rather than allowing multiple requests to be queued up on the servers
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				// After the first failure, try after a second
				BaseDelay: 1 * time.Second,
				// Don't keep the multiplier large since we want to resolve the connection ASAP (though prefer not
				// to try every second). Defaults to 1.6
				Multiplier: 1.1,
				// Backoff factor, specifically this controls -
				// nextRetryTime = now() + delay + RAND(-jitter * delay, jitter * delay), where delay = previous delay * multiplier
				Jitter: 0.2,
			},
			// Timeout if we are not able to establish a connection after a second on each attempt
			MinConnectTimeout: 1 * time.Second,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("could not connect to nitrous: %w", err)
	}
	return NitrousClient{
		Scope:  resource.NewPlaneScope(cfg.TierID),
		reader: rpc.NewNitrousClient(conn),
		binlog: cfg.BinlogProducer,
		binlogPartitions: cfg.BinlogPartitions,
		aggregateConf: cfg.AggregateConfProducer,
	}, nil
}
