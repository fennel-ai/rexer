package server

import (
	"container/ring"
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	libkakfa "fennel/kafka"
	"fennel/lib/feature"
	"fennel/tier"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

// The inspector server starts a goroutine that tails the feature log kafka topic
// and maintains a ring buffer of the last N feature logs. When a new request for
// recent features is received, it can be served from the ring buffer.
// We can extend this design to tail other kafka topics as well.
// Concerns:
// - This design incurs a lot of message reading overhead since the consumer is
// continuously reading from the topic even though most of the messages are discarded.
// An alternative design would be to create the consumer on-demand that starts reading
// from the "latest" offset and tails the topic for N new messages.
// Future:
// - Periodically (e.g. every 5 mins), save the current offset number (associated
// with the timestamp in the feature log message itself) to a db. When a request
// for features since a particular timestamp is received, create a new consumer
// at the nearest offset to the given timestamp and read N messages.
// - Allow streaming incoming messages over a websocket to the client.

type InspectorArgs struct {
	NumRecent uint `arg:"--num-recent" default:"10"`
}

type server struct {
	tier tier.Tier
	r    *ring.Ring
}

type entry struct {
	msg []byte
	ts  time.Time
}

func NewInspector(tr tier.Tier, args InspectorArgs) server {
	s := server{
		tier: tr,
		r:    ring.New(int(args.NumRecent)),
	}
	s.startFeatureLogTailer()
	return s
}

func (s *server) startFeatureLogTailer() error {
	topic := feature.KAFKA_TOPIC_NAME
	// Start tailing topic from the most recent messages.
	// We don't commit the offsets for this consumer since we always want to
	// read from the latest offset.
	consumer, err := s.tier.NewKafkaConsumer(libkakfa.ConsumerConfig{
		Topic:        topic,
		GroupID:      "featurelog_inspector",
		OffsetPolicy: libkakfa.LatestOffsetPolicy,
	})
	if err != nil {
		return fmt.Errorf("failed to start kafka consumer: %w", err)
	}
	go func(s *server, consumer libkakfa.FConsumer) {
		defer consumer.Close()
		ctx := context.Background()
		for {
			msgs, err := consumer.ReadBatch(ctx, 1000, time.Second*10)
			if err != nil {
				s.tier.Logger.Error("Error reading from kafka:", zap.Error(err))
				continue
			}
			if len(msgs) > 0 {
				s.tier.Logger.Debug("Got featurelog messages", zap.Int("count", len(msgs)))
				s.processMessages(msgs)
			}
		}
	}(s, consumer)
	return nil
}

func (s *server) SetHandlers(router *mux.Router) {
	router.HandleFunc("/features", s.getRecentFeatureLogs)
}

func (s *server) processMessages(msgs [][]byte) {
	// Keep only the messages that can fit in the ring.
	retained := len(msgs) % s.r.Len()
	if retained == 0 {
		retained = s.r.Len()
	}
	msgs = msgs[len(msgs)-retained:]
	now := time.Now()
	for _, msg := range msgs {
		s.r = s.r.Next()
		s.r.Value = entry{
			msg: msg,
			ts:  now,
		}
	}
}

func (s *server) getRecentMessages() []entry {
	start := s.r.Value
	if start == nil {
		return nil
	}
	var entries []entry
	entries = append(entries, start.(entry))
	curr := s.r
	for i := 1; i < s.r.Len(); i++ {
		curr = curr.Prev()
		if curr.Value == nil {
			break
		}
		entries = append(entries, curr.Value.(entry))
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].ts.After(entries[j].ts)
	})
	return entries
}

func (s *server) getRecentFeatureLogs(w http.ResponseWriter, r *http.Request) {
	entries := s.getRecentMessages()
	_, err := w.Write([]byte("["))
	if err != nil {
		s.tier.Logger.Error("Error writing to client:", zap.Error(err))
		w.WriteHeader(http.StatusResetContent)
		return
	}
	for i, e := range entries {
		_, err := w.Write(e.msg)
		if err != nil {
			s.tier.Logger.Error("Error writing to client:", zap.Error(err))
			w.WriteHeader(http.StatusResetContent)
			return
		}
		if i < len(entries)-1 {
			_, err = w.Write([]byte(","))
			if err != nil {
				s.tier.Logger.Error("Error writing to client:", zap.Error(err))
				w.WriteHeader(http.StatusResetContent)
				return
			}
		}
	}
	_, err = w.Write([]byte("]"))
	if err != nil {
		s.tier.Logger.Error("Error writing to client:", zap.Error(err))
		w.WriteHeader(http.StatusResetContent)
		return
	}
}
