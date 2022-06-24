package client

// type LocalConfig struct {
// 	tierID ftypes.RealmID
// 	Map    *map[string]map[string]string
// }

// func (l LocalConfig) Materialize() (resource.Resource, error) {
// 	return localClient{
// 		config: &l,
// 	}, nil
// }

// var _ resource.Config = &LocalConfig{}

// type localClient struct {
// 	config *LocalConfig
// }

// func (l localClient) Close() error {
// 	return nil
// }

// func (l localClient) Type() resource.Type {
// 	return resource.NitrousClient
// }

// func (l localClient) ID() ftypes.RealmID {
// 	return l.config.tierID
// }

// func (l localClient) PrefixedName(s string) string {
// 	return fmt.Sprintf("%d.%s", l.config.tierID, s)
// }

// func (l localClient) GetMany(ctx context.Context, reqs []nitrous.GetReq) ([]nitrous.GetResp, error) {
// 	ret := make([]nitrous.GetResp, len(reqs))
// 	for i, req := range reqs {
// 		k := l.PrefixedName(req.Base)
// 		if _, ok := (*l.config.Map)[k]; !ok {
// 			map_ := make(map[string]string)
// 			(*l.config.Map)[k] = map_
// 		}
// 		resp := nitrous.GetResp{Base: req.Base, Data: make(map[string]string), TierID: l.ID()}
// 		for _, inner := range req.Indices {
// 			if v, ok := (*l.config.Map)[k][inner]; ok {
// 				resp.Data[inner] = v
// 			}
// 		}
// 		ret[i] = resp
// 	}
// 	return ret, nil
// }

// func (l localClient) DelMany(ctx context.Context, reqs []nitrous.DelReq) error {
// 	for _, req := range reqs {
// 		k := l.PrefixedName(req.Base)
// 		if _, ok := (*l.config.Map)[k]; !ok {
// 			map_ := make(map[string]string)
// 			(*l.config.Map)[k] = map_
// 		}
// 		for _, inner := range req.Indices {
// 			delete((*l.config.Map)[k], inner)
// 		}
// 	}
// 	return nil
// }

// func (l localClient) SetMany(ctx context.Context, reqs []nitrous.SetReq) error {
// 	for _, req := range reqs {
// 		k := l.PrefixedName(req.Base)
// 		if _, ok := (*l.config.Map)[k]; !ok {
// 			map_ := make(map[string]string)
// 			(*l.config.Map)[k] = map_
// 		}
// 		for inner, v := range req.Data {
// 			(*l.config.Map)[k][inner] = v
// 			(*l.config.Map)[k][inner] = v
// 		}
// 	}
// 	return nil
// }

// func (l localClient) Init(ctx context.Context) error {
// 	return nil
// }

// func (l localClient) Lag(ctx context.Context) (uint64, error) {
// 	return 0, nil
// }

// var _ nitrous.Client = &localClient{}
