package kafka

import (
	"encoding/json"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var producerGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "kafka_producers_msg",
	Help: "Stats about kafka producer queues",
}, []string{"name", "metric"})

func extractString(unk interface{}) string {
	switch i := unk.(type) {
	case string:
		return i
	default:
		// not implemented for non-ints right now
		log.Printf("[WARNING]: type conversion not implemented for: %+v", i)
	}
	return ""
}

func extractValue(unk interface{}) float64 {
	switch i := unk.(type) {
	case float64:
		return i
	case int:
		return float64(i)
	default:
		// not implemented for non-ints right now
		log.Printf("[WARNING]: type conversion not implemented for: %+v", i)
	}
	return 0
}

func RecordEvents(eventCh chan kafka.Event) {
	for ev := range eventCh {
		switch e := ev.(type) {
		case *kafka.Message:
			// Delivery report handler for produced messages
			// This starts a go-routine that goes through all "delivery reports" for sends
			// as they arrive and logs if any of those are failing
			if e.TopicPartition.Error != nil {
				log.Printf("[ERROR] Kafka send failed. Event: %v", e.String())
			}
		case *kafka.Stats:
			// the stats are reported as string jsons
			var raw map[string]interface{}
			err := json.Unmarshal([]byte(e.String()), &raw)
			if err != nil {
				log.Printf("[WARNING] Failed to parse JSON from: %v", e.String())
				continue
			}
			// https://github.com/edenhill/librdkafka/blob/master/STATISTICS.md
			// Current number of messages in producer queues
			producerGauge.WithLabelValues(extractString(raw["name"]), "msg_cnt").Set(extractValue(raw["msg_cnt"]))
			// Threshold: maximum number of messages allowed on the producer queues
			producerGauge.WithLabelValues(extractString(raw["name"]), "msg_max").Set(extractValue(raw["msg_max"]))
			// Current total size of messages in producer queues
			producerGauge.WithLabelValues(extractString(raw["name"]), "msg_size").Set(extractValue(raw["msg_size"]))
			// Threshold: maximum total size of messages allowed on the producer queues
			producerGauge.WithLabelValues(extractString(raw["name"]), "msg_size_max").Set(extractValue(raw["msg_size_max"]))
		default:
			// not required
		}
	}
}
