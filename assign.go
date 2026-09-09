package fold

import "github.com/sharathb5/sharathfold/internal/hash"

// NodeIndex returns which of nodeCount nodes owns subscriberID under the
// multi-node assignment used by manifold/ (DECISIONS D12). Matches
// PartitionsForNode: node i claims partitions where p % nodeCount == i.
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
