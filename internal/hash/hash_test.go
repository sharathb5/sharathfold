package hash_test

import (
	"testing"

	"github.com/sharathb5/sharathfold/internal/hash"
)

func TestPartitionStable(t *testing.T) {
	const parts = hash.DefaultPartitions
	a := hash.Partition("sub-abc", parts)
	b := hash.Partition("sub-abc", parts)
	if a != b {
		t.Fatalf("unstable: %d vs %d", a, b)
	}
	if a < 0 || a >= parts {
		t.Fatalf("out of range: %d", a)
	}
}

func TestPartitionSpreads(t *testing.T) {
	seen := map[int]bool{}
	for i := 0; i < 500; i++ {
		p := hash.Partition("subscriber-"+string(rune('A'+i%50))+string(rune(i)), hash.DefaultPartitions)
		seen[p] = true
	}
	if len(seen) < 10 {
		t.Fatalf("expected some spread across partitions, got %d distinct", len(seen))
	}
}
