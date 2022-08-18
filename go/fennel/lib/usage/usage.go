package usage

const (
	HOURLY_USAGE_LOG_KAFKA_TOPIC = "hourly_usage_log"
)

func FromUsageCountersProto(proto *UsageCountersProto) *UsageCountersDBItem {
	return &UsageCountersDBItem{
		Queries:   proto.Queries,
		Actions:   proto.Actions,
		Timestamp: proto.Timestamp,
	}
}

// This is what gets persisted in DB at an hourly granularity.
type UsageCountersDBItem struct {
	// Number of queries.
	Queries uint64 `db:"queries"`
	// Number of actions.
	Actions uint64 `db:"actions"`
	// Timestamp in epoch in UTC.
	Timestamp uint64 `db:"timestamp"`
}

func ToUsageCountersProto(b *UsageCountersDBItem) *UsageCountersProto {
	return &UsageCountersProto{
		Queries:   b.Queries,
		Actions:   b.Actions,
		Timestamp: b.Timestamp,
	}
}
