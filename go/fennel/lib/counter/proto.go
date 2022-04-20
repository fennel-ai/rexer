package counter

import (
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func FromProtoAggregateDelta(pa *ProtoAggregateDelta) (AggregateDelta, error) {
	buckets := make([]Bucket, len(pa.Buckets))
	for i, pb := range pa.Buckets {
		b, err := FromProtoBucket(pb)
		if err != nil {
			return AggregateDelta{}, nil
		}
		buckets[i] = b
	}
	return AggregateDelta{
		AggId:   ftypes.AggId(pa.AggId),
		Buckets: buckets,
		Options: aggregate.FromProtoOptions(pa.Options),
	}, nil
}

func ToProtoAggregateDelta(aggId ftypes.AggId, options aggregate.Options, buckets []Bucket) (ProtoAggregateDelta, error) {
	pbs := make([]*ProtoBucket, len(buckets))
	for i, b := range buckets {
		pb, err := ToProtoBucket(b)
		if err != nil {
			return ProtoAggregateDelta{}, nil
		}
		pbs[i] = pb
	}
	return ProtoAggregateDelta{
		AggId:   uint32(aggId),
		Buckets: pbs,
		Options: aggregate.ToProtoOptions(options),
	}, nil
}

func ToProtoBucket(bucket Bucket) (*ProtoBucket, error) {
	pb := ProtoBucket{
		Key:    bucket.Key,
		Width:  bucket.Width,
		Index:  bucket.Index,
		Window: int32(bucket.Window),
	}
	v, err := value.ToProtoValue(bucket.Value)
	if err != nil {
		return &ProtoBucket{}, nil
	}
	pb.Value = &v
	return &pb, nil
}

func FromProtoBucket(bp *ProtoBucket) (Bucket, error) {
	b := Bucket{
		Key:    bp.Key,
		Width:  bp.Width,
		Index:  bp.Index,
		Window: ftypes.Window(bp.Window),
	}
	v, err := value.FromProtoValue(bp.Value)
	if err != nil {
		return Bucket{}, nil
	}
	b.Value = v
	return b, nil
}
