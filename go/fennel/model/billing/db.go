package billing

import (
	"context"
	"fmt"
	"strings"

	billinglib "fennel/lib/billing"
	"fennel/lib/timer"
	"fennel/tier"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

//================================================
// Public API for billing model
//================================================

// This is what gets returned to Billing queries.
type BillingCountersOverTimeRange struct {
	// Number of queries.
	Queries uint64 `json:"queries" db:"queries"`
	// Number of actions.
	Actions uint64 `json:"actions" db:"actions"`
	// Timestamp in epoch in UTC.
	StartTime uint64 `json:"startTime" db:"startTime"`
	// Timestamp in epoch in UTC.
	EndTime uint64 `json:"endTime" db:"endTime"`
}

func GetBillingCounters(ctx context.Context, tier tier.Tier, startTime uint64, endTime uint64) (*BillingCountersOverTimeRange, error) {
	return dbProvider{}.getBillingCounters(ctx, tier, startTime, endTime)
}

func InsertBillingCounters(ctx context.Context, tier tier.Tier, items []*billinglib.BillingCountersDBItem) error {
	return dbProvider{}.insertBillingCounters(ctx, tier, items)
}

// we create a private interface to make testing caching easier
type provider interface {
	getBillingCounters(ctx context.Context, tier tier.Tier, startTime uint64, endTime uint64) (*BillingCountersOverTimeRange, error)
	insertBillingCounters(ctx context.Context, tier tier.Tier, items []*billinglib.BillingCountersDBItem) error
}

type dbProvider struct{}

var _ provider = dbProvider{}

func (D dbProvider) getBillingCounters(ctx context.Context, tier tier.Tier, startTime uint64, endTime uint64) (*BillingCountersOverTimeRange, error) {
	ctx, t := timer.Start(ctx, tier.ID, "model.billing.db.getBillingCounters")
	defer t.Stop()
	sql := `select IFNULL(sum(queries), 0) as queries, IFNULL(sum(actions), 0) as actions, IFNULL(min(timestamp), ?) as startTime, IFNULL(max(timestamp) + 1, ?) as endTime from billing_counters where timestamp >= ? and timestamp < ?`
	billingCounters := make([]BillingCountersOverTimeRange, 0, 1)
	vals := make([]any, 0, 4)
	vals = append(vals, startTime, endTime, startTime, endTime)
	var err error
	if err = tier.DB.SelectContext(ctx, &billingCounters, sql, vals...); err != nil {
		return nil, err
	}
	if len(billingCounters) != 1 {
		return nil, fmt.Errorf("internal error in getting billing counters: expected exactly one count got `%v`", len(billingCounters))
	}
	return &billingCounters[0], nil
}

func (D dbProvider) insertBillingCounters(ctx context.Context, tier tier.Tier, items []*billinglib.BillingCountersDBItem) error {
	ctx, t := timer.Start(ctx, tier.ID, "model.billing.db.incBillingCounters")
	defer t.Stop()
	sql := `insert into billing_counters values`
	vals := make([]any, 0, len(items))
	for _, item := range items {
		sql += "(?, ?, ?),"
		vals = append(vals, item.Queries, item.Actions, item.Timestamp)
	}
	sql = strings.TrimSuffix(sql, ",") // remove the last trailing comma
	_, err := tier.DB.ExecContext(ctx, sql, vals...)
	return err
}
