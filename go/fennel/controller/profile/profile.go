package profile

import (
	"context"
	"time"

	"fennel/kafka"
	profilelib "fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/model/profile"
	"fennel/tier"

	"google.golang.org/protobuf/proto"
)

func Get(ctx context.Context, tier tier.Tier, pk profilelib.ProfileItemKey) (profilelib.ProfileItem, error) {
	defer timer.Start(ctx, tier.ID, "controller.profile.get").Stop()
	return profile.Get(ctx, tier, pk)
}

func Set(ctx context.Context, tier tier.Tier, request profilelib.ProfileItem) error {
	defer timer.Start(ctx, tier.ID, "controller.profile.set").Stop()
	if err := request.Validate(); err != nil {
		return err
	}
	if request.UpdateTime == 0 {
		request.UpdateTime = uint64(time.Now().UnixMicro())
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

	if err := profile.Set(ctx, tier, profilelib.NewProfileItem(string(request.OType), request.Oid, request.Key, request.Value, request.UpdateTime)); err != nil {
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
		if profile.UpdateTime == 0 {
			profile.UpdateTime = uint64(time.Now().UnixMicro())
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

func readBatch(ctx context.Context, consumer kafka.FConsumer, count int, timeout time.Duration) ([]profilelib.ProfileItem, error) {
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
	profiles, err := readBatch(ctx, consumer, 950, time.Second*10)
	if err != nil {
		return err
	}
	if len(profiles) == 0 {
		return nil
	}
	if err = profile.SetBatch(ctx, tr, profiles); err != nil {
		return err
	}
	return consumer.Commit()
}

// If profile item doesn't exist and hence the value, is not found, profileItem with value nil is returned.
func GetBatch(ctx context.Context, tier tier.Tier, requests []profilelib.ProfileItemKey) ([]profilelib.ProfileItem, error) {
	return profile.GetBatch(ctx, tier, requests)
}
