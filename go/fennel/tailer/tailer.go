//go:build badger

package tailer

import (
	"context"
	"fmt"

	"fennel/controller/profile"
	libkakfa "fennel/kafka"
	"fennel/lib/badger"
	profilelib "fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/model/offsets"
	"fennel/tier"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	db "github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
)

func Run(t tier.Tier) error {
	closeCh := make(chan struct{})
	t.Logger.Info("Tailer started")
	err := writeProfilesToLocalKvStore(t, closeCh)
	if err != nil {
		return err
	}
	return nil
}

func writeProfilesToLocalKvStore(tr tier.Tier, cancel <-chan struct{}) error {
	topic := profilelib.PROFILELOG_KAFKA_TOPIC
	var partitions kafka.TopicPartitions
	err := tr.Badger.View(func(txn *db.Txn) error {
		reader := badger.NewTransactionalStore(tr, txn)
		// TODO: This is *very* tightly coupled to topic prefixing.
		// Figure out a way of cleaning this up.
		topicName := tr.Badger.Scope.PrefixedName(topic)
		offs, err := offsets.Get(context.Background(), topicName, reader)
		if err != nil {
			return fmt.Errorf("failed to get ckpt offsets from badger: %v", err)
		}
		partitions = append(partitions, offs...)
		return nil
	})
	if err != nil {
		return err
	}
	consumer, err := tr.NewKafkaConsumer(libkakfa.ConsumerConfig{
		Topic: topic,
		// TODO(abhay): Use a group id that is unique to this instance of tailer.
		GroupID:    "_put_profiles_in_kv_store",
		Partitions: partitions,
		// If offsets are not specified, use default offset policy of reading from
		// earliest offset in partitions assigned by the broker.
		OffsetPolicy: libkakfa.EarliestOffsetPolicy,
	})
	if err != nil {
		return fmt.Errorf("unable to start consumer for inserting profiles in DB: %v", err)
	}
	go func(tr tier.Tier, consumer libkakfa.FConsumer) {
		defer consumer.Close()
		ctx := context.Background()
		for {
			select {
			case <-cancel:
				return
			default:
				t := timer.Start(ctx, tr.ID, "tailer.TransferProfilesToDB")
				if err := profile.TransferToDB(ctx, tr, consumer); err != nil {
					tr.Logger.Error("error while reading/writing actions to insert in db:", zap.Error(err))
				}
				t.Stop()
			}
		}
	}(tr, consumer)
	return nil
}
