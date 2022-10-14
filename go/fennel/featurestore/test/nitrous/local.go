package nitrous

import (
	"testing"

	"fennel/lib/ftypes"
	"fennel/nitrous/client"
	"fennel/nitrous/featurestore/test"
	"fennel/nitrous/rpc"

	"github.com/stretchr/testify/assert"
)

func NewLocalClient(t *testing.T, tierId ftypes.RealmID) (*rpc.Server, client.NitrousClient) {
	n := test.NewTestNitrous(t)
	server, addr := StartNitrousServer(t, n.Nitrous)
	config := client.NitrousClientConfig{
		TierID:                tierId,
		ServerAddr:            addr.String(),
		BinlogProducer:        n.NewBinlogProducer(t),
		BinlogPartitions:      1,
		ReqsLogProducer:       n.NewReqLogProducer(t),
		AggregateConfProducer: n.NewAggregateConfProducer(t),
	}
	r, err := config.Materialize()
	assert.NoError(t, err)
	return server, r.(client.NitrousClient)
}
