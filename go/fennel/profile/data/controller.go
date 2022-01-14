package data

import (
	"fennel/profile/lib"
	"fennel/value"
	"google.golang.org/protobuf/proto"
	"time"
)

type Controller struct {
	provider Provider
}

func NewController(p Provider) Controller {
	return Controller{p}
}

func (pc Controller) Init() error {
	return pc.provider.Init()
}

func (pc Controller) Get(request lib.ProfileItem) (*value.Value, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	valueSer, err := pc.provider.Get(request.OType, request.Oid, request.Key, request.Version)
	if err != nil {
		return nil, err
	} else if valueSer == nil {
		// i.e. no error but also value found
		return nil, nil
	}
	var pval value.PValue
	if err = proto.Unmarshal(valueSer, &pval); err != nil {
		return nil, err
	}
	val, err := value.FromProtoValue(&pval)
	if err != nil {
		return nil, err
	}
	return &val, nil
}

func (pc Controller) Set(request lib.ProfileItem) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if request.Version == 0 {
		request.Version = uint64(time.Now().Unix())
	}
	pval, err := value.ToProtoValue(request.Value)
	if err != nil {
		return err
	}
	valSer, err := proto.Marshal(&pval)
	if err != nil {
		return err
	}
	if err = pc.provider.Set(request.OType, request.Oid, request.Key, request.Version, valSer); err != nil {
		return err
	}
	return nil
}
