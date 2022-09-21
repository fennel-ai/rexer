package nitrous

import (
	"github.com/segmentio/fasthash/fnv1a"
)

func HashedPartition(groupKey string, numPartitions uint32) uint32 {
	return fnv1a.HashString32(groupKey) % numPartitions
}