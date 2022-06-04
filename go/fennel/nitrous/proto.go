package nitrous

import (
	"fennel/lib/ftypes"
	rpc "fennel/nitrous/rpc/v1"
	"fmt"
)

func FromProtoGetManyRequest(msg *rpc.GetManyRequest) ([]GetReq, error) {
	reqs := make([]GetReq, 0, len(msg.Keys))
	tierID := msg.GetTierID()
	for _, req := range msg.Keys {
		r := GetReq{
			TierID:  ftypes.RealmID(tierID),
			Base:    req.Key,
			Indices: req.Indices,
		}
		reqs = append(reqs, r)
	}
	return reqs, nil
}

func ToProtoGetManyRequest(reqs []GetReq) (*rpc.GetManyRequest, error) {
	ret := &rpc.GetManyRequest{}
	tierID := uint64(0)
	for _, req := range reqs {
		if tierID == 0 {
			tierID = uint64(req.TierID)
		} else if tierID != uint64(req.TierID) {
			return nil, fmt.Errorf("all requests must be for the same tier")
		}

		r := &rpc.Key{
			Key:     req.Base,
			Indices: req.Indices,
		}
		ret.Keys = append(ret.Keys, r)
	}
	ret.TierID = tierID
	return ret, nil
}

func FromProtoGetManyResponse(msg *rpc.GetManyResponse) ([]GetResp, error) {
	resp := make([]GetResp, 0, len(msg.Entries))
	for _, r := range msg.Entries {
		resp = append(resp, GetResp{
			TierID: ftypes.RealmID(msg.TierID),
			Base:   r.Key,
			Data:   r.Values,
		})
	}
	return resp, nil
}

func ToProtoGetManyResponse(msg []GetResp) (*rpc.GetManyResponse, error) {
	ret := &rpc.GetManyResponse{}
	tierID := uint64(0)
	for _, r := range msg {
		if tierID == 0 {
			tierID = uint64(r.TierID)
		} else if tierID != uint64(r.TierID) {
			return nil, fmt.Errorf("all responses must be for the same tier")
		}
		rpcResp := &rpc.Entry{
			Key:    r.Base,
			Values: r.Data,
		}
		ret.Entries = append(ret.Entries, rpcResp)
	}
	ret.TierID = tierID
	return ret, nil
}

func FromProtoSetRequest(msg *rpc.SetRequest) (SetReq, error) {
	return SetReq{
		TierID:  ftypes.RealmID(msg.TierID),
		Base:    msg.Entry.Key,
		Data:    msg.Entry.Values,
		Expires: ftypes.Timestamp(msg.Expires),
	}, nil
}

func ToProtoSetRequest(msg SetReq) (*rpc.SetRequest, error) {
	ret := &rpc.SetRequest{}
	ret.TierID = uint64(msg.TierID)
	rpcEntry := &rpc.Entry{
		Key:    msg.Base,
		Values: msg.Data,
	}
	ret.Entry = rpcEntry
	ret.Expires = uint64(msg.Expires)
	return ret, nil
}

func FromProtoDelRequest(msg *rpc.DelRequest) (DelReq, error) {
	return DelReq{
		TierID:  ftypes.RealmID(msg.TierID),
		Base:    msg.Key.Key,
		Indices: msg.Key.Indices,
	}, nil
}

func ToProtoDelRequest(msg DelReq) (*rpc.DelRequest, error) {
	ret := &rpc.DelRequest{}
	ret.TierID = uint64(msg.TierID)
	rpcKey := &rpc.Key{
		Key:     msg.Base,
		Indices: msg.Indices,
	}
	ret.Key = rpcKey
	return ret, nil
}
