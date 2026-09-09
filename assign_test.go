package fold_test

import (
	"fmt"
	"testing"

	"github.com/sharathb5/sharathfold"
	"github.com/sharathb5/sharathfold/internal/hash"
)

func TestNodeIndexMatchesPartitionMod(t *testing.T) {
	const partitions = 256
	const nodes = 2
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("sub-%d", i)
		p := hash.Partition(id, partitions)
		got := fold.NodeIndex(id, partitions, nodes)
		if got != p%nodes {
			t.Fatalf("%s: NodeIndex=%d want %d (partition=%d)", id, got, p%nodes, p)
		}
	}
}

func TestPartitionsForNodeDisjointCover(t *testing.T) {
	const partitions = 256
	const nodes = 2
	seen := map[int]int{}
	for n := 0; n < nodes; n++ {
		for _, p := range fold.PartitionsForNode(partitions, nodes, n) {
			if prev, ok := seen[p]; ok {
				t.Fatalf("partition %d claimed by nodes %d and %d", p, prev, n)
			}
			seen[p] = n
		}
	}
	if len(seen) != partitions {
		t.Fatalf("cover %d want %d", len(seen), partitions)
	}
}
