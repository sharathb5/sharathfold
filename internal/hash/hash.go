package hash

import "hash/fnv"

// DefaultPartitions is the fixed partition count. Subscribers hash onto these
// partitions; workers own partitions. Never hash modulo worker count.
const DefaultPartitions = 256

// Partition returns a stable partition in [0, partitions) for subscriberID.
// partitions must be > 0; callers pass DefaultPartitions in production.
func Partition(subscriberID string, partitions int) int {
	if partitions <= 0 {
		partitions = DefaultPartitions
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(subscriberID))
	return int(h.Sum64() % uint64(partitions))
}
