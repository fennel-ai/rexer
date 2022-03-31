package profile

import (
	"context"
	"fmt"
	"time"

	"fennel/kafka"
	profilelib "fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/lib/value"
	"fennel/model/profile"
	"fennel/tier"

	"google.golang.org/protobuf/proto"
)

func Get(ctx context.Context, tier tier.Tier, request profilelib.ProfileItem) (value.Value, error) {
	defer timer.Start(ctx, tier.ID, "controller.profile.get").Stop()
	if err := request.Validate(); err != nil {
		return nil, err
	}
	valueSer, err := profile.Get(ctx, tier, request.OType, request.Oid, request.Key, request.Version)
	if err != nil {
		return nil, err
	} else if valueSer == nil {
		// i.e. no error but also value found
		return nil, nil
	}
	val, err := value.FromJSON(valueSer)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func Set(ctx context.Context, tier tier.Tier, request profilelib.ProfileItem) error {
	defer timer.Start(ctx, tier.ID, "controller.profile.set").Stop()
	if err := request.Validate(); err != nil {
		return err
	}
	if request.Version == 0 {
		request.Version = uint64(time.Now().UnixMicro())
	}

	// write to Kafka to ensure that profile will be written eventually even if the set call here fails;
	// Since Kafka consumer will retry in case of failures, the cache will be eventually consistent
	// with the DB. However, in the sunny scenario, this may lead to multiple writes to the DB and cache.
	// Kafka consumer consumes and writes profiles in batch, the added latency of a double write is not linearly high
	p, err := profilelib.ToProtoProfileItem(&request)
	if err != nil {
		return err
	}
	producer := tier.Producers[profilelib.PROFILELOG_KAFKA_TOPIC]
	if err := producer.LogProto(ctx, &p, nil); err != nil {
		return err
	}

	valSer := value.ToJSON(request.Value)
	if err := profile.Set(ctx, tier, request.OType, request.Oid, request.Key, request.Version, valSer); err != nil {
		return err
	}
	return nil
}

func SetMulti(ctx context.Context, tier tier.Tier, request []profilelib.ProfileItem) error {
	defer timer.Start(ctx, tier.ID, "controller.profile.setmulti").Stop()
	profiles := make([]*profilelib.ProtoProfileItem, 0)
	for _, profile := range request {
		if err := profile.Validate(); err != nil {
			return err
		}
		if profile.Version == 0 {
			profile.Version = uint64(time.Now().UnixMicro())
		}
		protoVal, err := profilelib.ToProtoProfileItem(&profile)
		if err != nil {
			return err
		}
		profiles = append(profiles, &protoVal)
	}
	producer := tier.Producers[profilelib.PROFILELOG_KAFKA_TOPIC]
	// TODO: Define and implement batch logging once the downstream API moves out of experimental
	for _, p := range profiles {
		err := producer.LogProto(ctx, p, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func dbInsert(ctx context.Context, tier tier.Tier, profiles []profilelib.ProfileItem) error {
	profileSers := make([]profilelib.ProfileItemSer, len(profiles))
	for i, p := range profiles {
		if err := p.Validate(); err != nil {
			return fmt.Errorf("invalid action: %v", err)
		}
		if p.Version == 0 {
			p.Version = uint64(time.Now().UnixMicro())
		}
		pSer := p.ToProfileItemSer()
		profileSers[i] = *pSer
	}
	return profile.SetBatch(ctx, tier, profileSers)
}

func ReadBatch(ctx context.Context, consumer kafka.FConsumer, count int, timeout time.Duration) ([]profilelib.ProfileItem, error) {
	msgs, err := consumer.ReadBatch(ctx, count, timeout)
	if err != nil {
		return nil, err
	}
	profiles := make([]profilelib.ProfileItem, len(msgs))
	for i := range msgs {
		var p profilelib.ProtoProfileItem
		if err = proto.Unmarshal(msgs[i], &p); err != nil {
			return nil, err
		}
		if profiles[i], err = profilelib.FromProtoProfileItem(&p); err != nil {
			return nil, err
		}
	}
	return profiles, nil
}

func TransferToDB(ctx context.Context, tr tier.Tier, consumer kafka.FConsumer) error {
	profiles, err := ReadBatch(ctx, consumer, 950, time.Second*10)
	if err != nil {
		return err
	}
	if len(profiles) == 0 {
		return nil
	}
	if err = dbInsert(ctx, tr, profiles); err != nil {
		return err
	}
	return consumer.Commit()
}

// GetBatched takes a list of profile items (value field is ignored) and returns a list of values
// corresponding to the value of each profile item. If profile item doesn't exist and hence the value
// is not found, nil is returned instead
func GetBatched(ctx context.Context, tier tier.Tier, requests []profilelib.ProfileItem) ([]value.Value, error) {
	sers, err := profile.GetBatched(ctx, tier, requests)
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, len(sers))
	for i := range sers {
		// if we don't have this data stored, well just return a nil
		if sers[i] == nil {
			ret[i] = nil
		} else {
			ret[i], err = value.FromJSON(sers[i])
			if err != nil {
				return nil, err
			}
		}
	}
	return ret, nil
}

func GetMulti(ctx context.Context, tier tier.Tier, request profilelib.ProfileFetchRequest) ([]profilelib.ProfileItem, error) {
	profilesSer, err := profile.GetMulti(ctx, tier, request)
	if err != nil {
		return nil, err
	}

	profiles, err := profilelib.FromProfileItemSerList(profilesSer)
	if err != nil {
		return nil, err
	}
	return profiles, nil
}
