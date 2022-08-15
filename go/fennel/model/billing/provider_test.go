package billing

import (
	"context"
	"testing"

	billinglib "fennel/lib/billing"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func testInsertAndQuery(t *testing.T, p provider) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	startTime := uint64(tier.Clock.Now())
	endTime := startTime + 1
	items := make([]*billinglib.BillingCountersDBItem, 4)
	for i := 0; i < 4; i++ {
		items[i] = &billinglib.BillingCountersDBItem{
			Queries:   3,
			Actions:   3,
			Timestamp: startTime,
		}
	}
	assert.NoError(t, p.insertBillingCounters(ctx, tier, items))
	billingCounters, err := p.getBillingCounters(ctx, tier, startTime, endTime)
	assert.NoError(t, err)
	assert.Equal(t, uint64(12), billingCounters.Queries)
	assert.Equal(t, uint64(12), billingCounters.Queries)
	assert.Equal(t, startTime, billingCounters.StartTime)
	assert.Equal(t, endTime, billingCounters.EndTime)
}
