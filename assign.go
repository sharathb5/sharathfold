package fold

import (
	"fmt"

	"github.com/sharathb5/sharathfold/internal/hash"
)

// LogicalNodeID is the stable Postgres owner_node / Config.NodeID for a
// manifold fleet member (go0, go1, …). Independent of the Erlang node name.
func LogicalNodeID(nodeIndex int) string {
	return fmt.Sprintf("go%d", nodeIndex)
}

// NodeIndex returns which of nodeCount nodes owns subscriberID under the
// initial multi-node assignment used by manifold/. Matches
// PartitionsForNode: node i claims partitions where p % nodeCount == i.
// After live handoff, durable fold_partition_owners is authoritative instead.
func NodeIndex(subscriberID string, partitions, nodeCount int) int {
	if nodeCount <= 0 {
		return 0
	}
	return hash.Partition(subscriberID, partitions) % nodeCount
}

// PartitionsForNode returns the disjoint partition slice claimed by nodeIndex
// in a nodeCount-node fleet. Empty when arguments are invalid.
func PartitionsForNode(partitions, nodeCount, nodeIndex int) []int {
	if partitions <= 0 {
		partitions = hash.DefaultPartitions
	}
	if nodeCount <= 0 || nodeIndex < 0 || nodeIndex >= nodeCount {
		return nil
	}
	out := make([]int, 0, (partitions/nodeCount)+1)
	for p := 0; p < partitions; p++ {
		if p%nodeCount == nodeIndex {
			out = append(out, p)
		}
	}
	return out
}

// OwnerAssignmentsForNodes returns owners[p] = LogicalNodeID(p % nodeCount)
// for seeding fold_partition_owners to match PartitionsForNode.
func OwnerAssignmentsForNodes(partitions, nodeCount int) []string {
	if partitions <= 0 {
		partitions = hash.DefaultPartitions
	}
	if nodeCount <= 0 {
		return nil
	}
	owners := make([]string, partitions)
	for p := 0; p < partitions; p++ {
		owners[p] = LogicalNodeID(p % nodeCount)
	}
	return owners
}
