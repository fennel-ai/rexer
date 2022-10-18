package nitrous

import (
	"testing"

	"fennel/lib/ftypes"
	"fennel/nitrous/client"
	"fennel/nitrous/rpc"
	"fennel/nitrous/test"

	"github.com/raulk/clock"
	"github.com/stretchr/testify/assert"
)

func NewLocalClient(t *testing.T, tierId ftypes.RealmID, clock clock.Clock) (*rpc.Server, client.NitrousClient) {
	n := test.NewTestNitrous(t)
	n.Clock = clock
	server, addr := StartNitrousServer(t, n.Nitrous)
	config := client.NitrousClientConfig{
		TierID:                tierId,
		ServerAddr:            addr.String(),
		BinlogProducer:        n.NewBinlogProducer(t),
		BinlogPartitions:      1,
		AggregateConfProducer: n.NewAggregateConfProducer(t),
	}
	r, err := config.Materialize()
	assert.NoError(t, err)
	return server, r.(client.NitrousClient)
}
