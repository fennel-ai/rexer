package billing

const (
	HOURLY_BILLING_LOG_KAFKA_TOPIC = "hourly_billing_log"
)

func FromBillingCountersProto(proto *BillingCountersProto) *BillingCountersDBItem {
	return &BillingCountersDBItem{
		Queries:   proto.Queries,
		Actions:   proto.Actions,
		Timestamp: proto.Timestamp,
	}
}

// This is what gets persisted in DB at an hourly granularity.
type BillingCountersDBItem struct {
	// Number of queries.
	Queries uint64 `db:"queries"`
	// Number of actions.
	Actions uint64 `db:"actions"`
	// Timestamp in epoch in UTC.
	Timestamp uint64 `db:"timestamp"`
}

func ToBillingCountersProto(b *BillingCountersDBItem) *BillingCountersProto {
	return &BillingCountersProto{
		Queries:   b.Queries,
		Actions:   b.Actions,
		Timestamp: b.Timestamp,
	}
}
