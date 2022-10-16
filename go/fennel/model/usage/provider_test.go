package usage

import (
	"context"
	"testing"

	usagelib "fennel/lib/usage"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func testInsertAndQuery(t *testing.T, p provider) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	startTime := uint64(tier.Clock.Now().Unix())
	endTime := startTime + 1
	items := make([]*usagelib.UsageCountersProto, 4)
	for i := 0; i < 4; i++ {
		items[i] = &usagelib.UsageCountersProto{
			Queries:   3,
			Actions:   3,
			Timestamp: startTime,
		}
	}
	assert.NoError(t, p.insertUsageCounters(ctx, tier, items))
	usageCounters, err := p.getUsageCounters(ctx, tier, startTime, endTime)
	assert.NoError(t, err)
	assert.Equal(t, uint64(12), usageCounters.Queries)
	assert.Equal(t, uint64(12), usageCounters.Queries)
	assert.Equal(t, startTime, usageCounters.StartTime)
	assert.Equal(t, endTime, usageCounters.EndTime)
}
