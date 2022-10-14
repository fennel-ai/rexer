package aggregate

import (
	"log"

	"fennel/lib/featurestore/aggregate/proto"
	"fennel/lib/featurestore/schema"
)

func FromRequest(req *proto.CreateAggregateRequest) Aggregate {
	return Aggregate{
		Name:         req.Name,
		Mode:         req.Mode,
		Version:      req.Version,
		Stream:       req.Stream,
		AggCls:       req.AggCls,
		Type:         fromProtoType(req.AggregateType),
		Windows:      req.Windows,
		Schema:       schema.FromProto(req.Schema),
		Dependencies: req.DependsOnAggregates,
	}
}

func fromProtoType(pt *proto.AggregateType) Type {
	var cfg Config
	switch pcfg := pt.Config.(type) {
	case *proto.AggregateType_WindowConfig:
		cfg = WindowConfig{
			Windows:    pcfg.WindowConfig.Windows,
			ValueField: pcfg.WindowConfig.ValueField,
		}
	case *proto.AggregateType_KeyValueConfig:
		cfg = KeyValueConfig{ValueField: pcfg.KeyValueConfig.ValueField}
	case *proto.AggregateType_TopkConfig:
		cfg = TopKConfig{
			K:          pcfg.TopkConfig.K,
			ItemFields: pcfg.TopkConfig.ItemFields,
			ScoreField: pcfg.TopkConfig.ScoreField,
		}
	case *proto.AggregateType_CfConfig:
		cfg = CFConfig{
			K:             pcfg.CfConfig.K,
			ContextFields: pcfg.CfConfig.ContextFields,
			WeightField:   pcfg.CfConfig.WeightField,
		}
	default:
		// this should never happen as all types are covered
		log.Println("aggregate.fromProtoType() found unknown type which should never happen")
		cfg = nil
	}
	return Type{
		Function:       int(pt.Function),
		KeyFields:      pt.KeyFields,
		TimestampField: pt.TimestampField,
		Config:         cfg,
	}
}
