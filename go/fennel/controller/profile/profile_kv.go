//go:build badger

package profile

import (
	"context"
	"fmt"
	"time"

	libkafka "fennel/kafka"
	"fennel/lib/badger"
	profilelib "fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/model/offsets"
	profilekv "fennel/model/profile/kv"
	"fennel/tier"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	db "github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func Get(ctx context.Context, tier tier.Tier, pk profilelib.ProfileItemKey) (profilelib.ProfileItem, error) {
	defer timer.Start(ctx, tier.ID, "controller.profile.get").Stop()
	items, err := GetBatch(ctx, tier, []profilelib.ProfileItemKey{pk})
	if err != nil {
		return profilelib.ProfileItem{}, err
	} else if len(items) == 0 {
		return profilelib.ProfileItem{}, fmt.Errorf("profile item not found")
	}
	return items[0], nil
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

func readBatch(ctx context.Context, consumer libkafka.FConsumer, count int, timeout time.Duration) ([]profilelib.ProfileItem, error) {
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

func TransferToDB(ctx context.Context, tr tier.Tier, consumer libkafka.FConsumer) error {
	profiles, err := readBatch(ctx, consumer, 950, time.Second*5)
	if err != nil {
		return errors.Wrap(err, "failed to read batch from kafka")
	}
	if len(profiles) == 0 {
		return nil
	}
	var partitions kafka.TopicPartitions
	err = tr.Badger.Update(func(txn *db.Txn) error {
		writer := badger.NewTransactionalStore(tr, txn)
		err = profilekv.Set(ctx, profiles, writer)
		if err != nil {
			return errors.Wrap(err, "failed to set profile items")
		}
		partitions, err = consumer.Offsets()
		if err != nil {
			return errors.Wrap(err, "failed to read current kafka offsets")
		}
		tr.Logger.Debug("Committing offsets", zap.Any("partitions", partitions))
		err = offsets.Set(ctx, tr.Logger, partitions, writer)
		if err != nil {
			return errors.Wrap(err, "failed to write offsets")
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "failed to write badger transaction")
	}
	// Sync contents to disk, just to be safe.
	err = tr.Badger.Sync()
	if err != nil {
		return errors.Wrap(err, "failed to sync badger to disk")
	}
	_, err = consumer.CommitOffsets(partitions)
	if err != nil {
		return errors.Wrap(err, "failed to commit offsets")
	}
	return nil
}

// If profile item doesn't exist and hence the value, is not found, profileItem with value nil is returned.
func GetBatch(ctx context.Context, tier tier.Tier, requests []profilelib.ProfileItemKey) ([]profilelib.ProfileItem, error) {
	ret := make([]profilelib.ProfileItem, 0, len(requests))
	err := tier.Badger.View(func(txn *db.Txn) error {
		reader := badger.NewTransactionalStore(tier, txn)
		profiles, err := profilekv.Get(ctx, requests, reader)
		if err != nil {
			return fmt.Errorf("failed to get profile items: %v", err)
		} else {
			ret = append(ret, profiles...)
			return nil
		}
	})
	return ret, err
}
