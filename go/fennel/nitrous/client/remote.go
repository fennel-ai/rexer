package client

import (
	"context"
	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/nitrous"
	"fennel/nitrous/rpc"
	"fennel/resource"
	"fmt"

	"google.golang.org/grpc"
)

type remoteConfig struct {
	host   string
	port   int
	binlog fkafka.FProducer
	tierID ftypes.RealmID
}

type localConfig struct {
	host   string
	port   int
	binlog fkafka.FProducer
	tierID ftypes.RealmID
}

func NewRemoteConfig(tierID ftypes.RealmID, host string, port int, binlog fkafka.FProducer) resource.Config {
	return remoteConfig{
		host:   host,
		port:   port,
		binlog: binlog,
		tierID: tierID,
	}
}

func (c remoteConfig) Materialize() (resource.Resource, error) {
	conn, err := grpc.Dial(fmt.Sprintf("http://%s:%d", c.host, c.port), grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &remoteClient{
		NitrousClient: rpc.NewNitrousClient(conn),
		config:        c,
	}, nil
}

var _ resource.Config = &remoteConfig{}

type remoteClient struct {
	config remoteConfig
	rpc.NitrousClient
}

func (c remoteClient) Close() error {
	return c.config.binlog.Close()
}

func (c remoteClient) Type() resource.Type {
	return resource.NitrousClient
}

func (c remoteClient) ID() ftypes.RealmID {
	return c.config.tierID
}

func (c remoteClient) PrefixedName(s string) string {
	len_ := 9 + 4 + len(s)
	buf := make([]byte, len_)
	sz := 0
	n, _ := binary.PutUvarint(buf[sz:], uint64(c.config.tierID))
	sz += n
	n, _ = binary.PutString(buf[sz:], s)
	sz += n
	return string(buf[:sz])
}

// SetMany encodes a set of key-value pairs and sends them to the Nitrous server via Kafka
// for async  processing.
func (c remoteClient) SetMany(ctx context.Context, reqs []nitrous.SetReq) error {
	for _, req := range reqs {
		preq, err := nitrous.ToProtoSetRequest(req)
		if err != nil {
			return err
		}
		if err = c.config.binlog.LogProto(ctx, preq, []byte(preq.Entry.Key)); err != nil {
			return err
		}
	}
	return nil
}

func (c remoteClient) DelMany(ctx context.Context, reqs []nitrous.DelReq) error {
	for _, req := range reqs {
		preq, err := nitrous.ToProtoDelRequest(req)
		if err != nil {
			return err
		}
		if err = c.config.binlog.LogProto(ctx, preq, []byte(preq.Key.Key)); err != nil {
			return err
		}
	}

	return nil
}

func (c remoteClient) GetMany(ctx context.Context, reqs []nitrous.GetReq) ([]nitrous.GetResp, error) {
	preq, err := nitrous.ToProtoGetManyRequest(reqs)
	if err != nil {
		return nil, err
	}
	res, err := c.NitrousClient.GetMany(ctx, preq)
	if err != nil {
		return nil, err
	}
	return nitrous.FromProtoGetManyResponse(res)
}

func (c remoteClient) Init(ctx context.Context) error {
	_, err := c.NitrousClient.Init(ctx, &rpc.InitReq{
		TierID: uint64(c.config.tierID),
	})
	return err
}

func (c remoteClient) Lag(ctx context.Context) (uint64, error) {
	resp, err := c.NitrousClient.Lag(ctx, &rpc.LagReq{
		TierID: uint64(c.config.tierID),
	})
	if err != nil {
		return 0, err
	}
	return resp.Lag, nil
}

var _ nitrous.Client = &remoteClient{}
