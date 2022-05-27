package nitrous

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/resource"
)

const (
	BINLOG_KAFKA_TOPIC = "nitrous_log"
)

type Client interface {
	resource.Resource
	Init(ctx context.Context) error
	GetMany(ctx context.Context, reqs []GetReq) ([]GetResp, error)
	DelMany(ctx context.Context, reqs []DelReq) error
	SetMany(ctx context.Context, reqs []SetReq) error
	Lag(ctx context.Context) (uint64, error)
}

type GetReq struct {
	TierID  ftypes.RealmID
	Base    string
	Indices []string
}

type GetResp struct {
	TierID ftypes.RealmID
	Base   string
	Data   map[string]string
}

type SetReq struct {
	TierID  ftypes.RealmID
	Base    string
	Data    map[string]string
	Expires ftypes.Timestamp // timestamp in seconds since epoch when this record expires
}

type DelReq GetReq
