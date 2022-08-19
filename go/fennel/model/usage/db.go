package usage

import (
	"context"
	"fmt"
	"strings"

	"fennel/lib/timer"
	usagelib "fennel/lib/usage"
	"fennel/tier"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

//================================================
// Public API for usage model
//================================================

// This is what gets returned to usage queries.
type UsageCountersOverTimeRange struct {
	// Number of queries.
	Queries uint64 `json:"queries" db:"queries"`
	// Number of actions.
	Actions uint64 `json:"actions" db:"actions"`
	// Timestamp in epoch in UTC.
	StartTime uint64 `json:"startTime" db:"startTime"`
	// Timestamp in epoch in UTC.
	EndTime uint64 `json:"endTime" db:"endTime"`
}

func GetUsageCounters(ctx context.Context, tier tier.Tier, startTime uint64, endTime uint64) (*UsageCountersOverTimeRange, error) {
	return dbProvider{}.getUsageCounters(ctx, tier, startTime, endTime)
}

func InsertUsageCounters(ctx context.Context, tier tier.Tier, items []*usagelib.UsageCountersProto) error {
	return dbProvider{}.insertUsageCounters(ctx, tier, items)
}

// we create a private interface to make testing caching easier
type provider interface {
	getUsageCounters(ctx context.Context, tier tier.Tier, startTime uint64, endTime uint64) (*UsageCountersOverTimeRange, error)
	insertUsageCounters(ctx context.Context, tier tier.Tier, items []*usagelib.UsageCountersProto) error
}

type dbProvider struct{}

var _ provider = dbProvider{}

func (D dbProvider) getUsageCounters(ctx context.Context, tier tier.Tier, startTime uint64, endTime uint64) (*UsageCountersOverTimeRange, error) {
	ctx, t := timer.Start(ctx, tier.ID, "model.usage.db.getusageCounters")
	defer t.Stop()
	sql := `select IFNULL(sum(queries), 0) as queries, IFNULL(sum(actions), 0) as actions, IFNULL(min(timestamp), ?) as startTime, IFNULL(max(timestamp) + 1, ?) as endTime from usage_counters where timestamp >= ? and timestamp < ?`
	usageCounters := make([]UsageCountersOverTimeRange, 0, 1)
	vals := make([]any, 0, 4)
	vals = append(vals, startTime, endTime, startTime, endTime)
	var err error
	if err = tier.DB.SelectContext(ctx, &usageCounters, sql, vals...); err != nil {
		return nil, err
	}
	if len(usageCounters) != 1 {
		return nil, fmt.Errorf("internal error in getting usage counters: expected exactly one count got `%v`", len(usageCounters))
	}
	return &usageCounters[0], nil
}

func (D dbProvider) insertUsageCounters(ctx context.Context, tier tier.Tier, items []*usagelib.UsageCountersProto) error {
	ctx, t := timer.Start(ctx, tier.ID, "model.usage.db.incusageCounters")
	defer t.Stop()
	sql := `insert into usage_counters values`
	vals := make([]any, 0, len(items))
	for _, item := range items {
		sql += "(?, ?, ?),"
		vals = append(vals, item.Queries, item.Actions, item.Timestamp)
	}
	sql = strings.TrimSuffix(sql, ",") // remove the last trailing comma
	_, err := tier.DB.ExecContext(ctx, sql, vals...)
	return err
}
